use anyhow::{anyhow, Context, Result};
use blake3::Hasher;
use chromaprint::Chromaprint;
use lofty::file::{AudioFile, FileType, TaggedFileExt};
use lofty::probe::Probe;
use rayon::prelude::*;
use serde::Serialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use symphonia::core::audio::SampleBuffer;
use symphonia::core::codecs::{DecoderOptions, CODEC_TYPE_NULL};
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;

const DURATION_BUCKET_MS: u64 = 250;
const MIN_AUDIO_DURATION_MS: u64 = 1_000;
const FINGERPRINT_WINDOW_SECONDS: u64 = 120;
const PCM_WINDOW_COUNT: usize = 3;
const PCM_WINDOW_SECONDS: u64 = 4;
const STEM_HINTS: &[&str] = &[
    "stem",
    "stems",
    "sample pack",
    "sample-pack",
    "samples",
    "multitrack",
    "multitracks",
    "acapella",
    "acapellas",
];

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AudioEntry {
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
    pub(crate) codec: String,
    pub(crate) duration_ms: u64,
    pub(crate) sample_rate: Option<u32>,
    pub(crate) bit_depth: Option<u8>,
    pub(crate) channels: Option<u8>,
    pub(crate) bitrate_kbps: Option<u32>,
    pub(crate) lossless: bool,
    pub(crate) quality_score: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AudioRecommendation {
    pub(crate) keep: PathBuf,
    pub(crate) delete: Vec<PathBuf>,
    pub(crate) reason: String,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct AudioDuplicateGroup {
    pub(crate) id: String,
    pub(crate) fingerprint: String,
    pub(crate) duration_ms: u64,
    pub(crate) entries: Vec<AudioEntry>,
    pub(crate) recommendation: AudioRecommendation,
}

#[derive(Debug)]
pub(crate) struct AudioAnalysisProgress {
    metadata_total: AtomicU64,
    metadata_done: AtomicU64,
    fingerprint_total: AtomicU64,
    fingerprint_done: AtomicU64,
    finished: AtomicBool,
}

impl AudioAnalysisProgress {
    pub(crate) fn new(metadata_total: usize) -> Arc<Self> {
        Arc::new(Self {
            metadata_total: AtomicU64::new(metadata_total as u64),
            metadata_done: AtomicU64::new(0),
            fingerprint_total: AtomicU64::new(0),
            fingerprint_done: AtomicU64::new(0),
            finished: AtomicBool::new(false),
        })
    }

    fn metadata_complete(&self) {
        self.metadata_done.fetch_add(1, Ordering::Relaxed);
    }

    fn set_metadata_total(&self, total: usize) {
        self.metadata_total.store(total as u64, Ordering::Relaxed);
    }

    fn set_fingerprint_total(&self, total: usize) {
        self.fingerprint_total
            .store(total as u64, Ordering::Relaxed);
    }

    fn fingerprint_complete(&self) {
        self.fingerprint_done.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn metadata_total(&self) -> u64 {
        self.metadata_total.load(Ordering::Relaxed)
    }

    pub(crate) fn metadata_done(&self) -> u64 {
        self.metadata_done.load(Ordering::Relaxed)
    }

    pub(crate) fn fingerprint_total(&self) -> u64 {
        self.fingerprint_total.load(Ordering::Relaxed)
    }

    pub(crate) fn fingerprint_done(&self) -> u64 {
        self.fingerprint_done.load(Ordering::Relaxed)
    }

    pub(crate) fn finish(&self) {
        self.finished.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Debug)]
struct AudioCandidate {
    entry: AudioEntry,
    fingerprint: String,
    pcm_signature: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ClusterKey {
    duration_bucket: u64,
    channels: u8,
}

fn codec_label(file_type: FileType, path: &Path) -> String {
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase());
    match (file_type, ext) {
        (FileType::Aac, _) => "aac".to_string(),
        (FileType::Aiff, _) => "aiff".to_string(),
        (FileType::Ape, _) => "ape".to_string(),
        (FileType::Flac, _) => "flac".to_string(),
        (FileType::Mpeg, Some(ext)) => ext,
        (FileType::Mpeg, None) => "mpeg".to_string(),
        (FileType::Mp4, Some(ext)) => ext,
        (FileType::Mp4, None) => "mp4".to_string(),
        (FileType::Mpc, _) => "mpc".to_string(),
        (FileType::Opus, _) => "opus".to_string(),
        (FileType::Vorbis, _) => "vorbis".to_string(),
        (FileType::Speex, _) => "speex".to_string(),
        (FileType::Wav, _) => "wav".to_string(),
        (FileType::WavPack, _) => "wavpack".to_string(),
        (FileType::Custom(label), _) => label.to_ascii_lowercase(),
        (_, Some(ext)) => ext,
        (_, None) => "audio".to_string(),
    }
}

fn is_lossless(file_type: FileType, bit_depth: Option<u8>, bitrate_kbps: Option<u32>) -> bool {
    match file_type {
        FileType::Flac | FileType::Wav | FileType::Aiff | FileType::Ape | FileType::WavPack => true,
        FileType::Mp4 => bit_depth.unwrap_or(0) >= 16 && bitrate_kbps.unwrap_or(0) == 0,
        _ => false,
    }
}

fn quality_score(
    lossless: bool,
    bit_depth: Option<u8>,
    sample_rate: Option<u32>,
    bitrate_kbps: Option<u32>,
) -> u64 {
    let lossless_score = if lossless { 1_000_000_000u64 } else { 0 };
    let depth_score = bit_depth.unwrap_or(0) as u64 * 1_000_000;
    let sample_rate_score = sample_rate.unwrap_or(0) as u64 * 1_000;
    let bitrate_score = bitrate_kbps.unwrap_or(0) as u64 * 10;
    lossless_score + depth_score + sample_rate_score + bitrate_score
}

fn path_contains_stem_hint(path: &Path) -> bool {
    let lower = path.to_string_lossy().to_ascii_lowercase();
    STEM_HINTS.iter().any(|hint| lower.contains(hint))
}

fn audio_entry(path: &Path, size: u64) -> Result<AudioEntry> {
    let tagged_file = Probe::open(path)
        .with_context(|| format!("failed opening audio file {}", path.display()))?
        .read()
        .with_context(|| format!("failed reading audio metadata {}", path.display()))?;
    let properties = tagged_file.properties();
    let duration_ms = properties.duration().as_millis() as u64;
    if duration_ms < MIN_AUDIO_DURATION_MS {
        return Err(anyhow!("audio duration too short"));
    }
    let sample_rate = properties.sample_rate();
    let bit_depth = properties.bit_depth();
    let channels = properties.channels();
    let bitrate_kbps = properties.audio_bitrate().or(properties.overall_bitrate());
    let lossless = is_lossless(tagged_file.file_type(), bit_depth, bitrate_kbps);
    let codec = codec_label(tagged_file.file_type(), path);
    let score = quality_score(lossless, bit_depth, sample_rate, bitrate_kbps);

    Ok(AudioEntry {
        path: path.to_path_buf(),
        size,
        codec,
        duration_ms,
        sample_rate,
        bit_depth,
        channels,
        bitrate_kbps,
        lossless,
        quality_score: score,
    })
}

fn target_window_starts(total_samples: usize) -> Vec<usize> {
    let window_len = PCM_WINDOW_SECONDS as usize;
    if total_samples <= window_len {
        return vec![0];
    }
    let max_start = total_samples.saturating_sub(window_len);
    let mut starts = Vec::new();
    for idx in 0..PCM_WINDOW_COUNT {
        let start = if PCM_WINDOW_COUNT == 1 {
            0
        } else {
            max_start.saturating_mul(idx) / (PCM_WINDOW_COUNT - 1)
        };
        starts.push(start);
    }
    starts.sort_unstable();
    starts.dedup();
    starts
}

fn hash_pcm_windows(window_buffers: &[Vec<i16>]) -> String {
    let mut hasher = Hasher::new();
    for window in window_buffers {
        hasher.update(&(window.len() as u64).to_le_bytes());
        for sample in window {
            hasher.update(&sample.to_le_bytes());
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn fingerprint_audio(path: &Path, duration_ms: u64) -> Result<(String, String)> {
    let src =
        File::open(path).with_context(|| format!("failed opening audio {}", path.display()))?;
    let mss = MediaSourceStream::new(Box::new(src), Default::default());

    let mut hint = Hint::new();
    if let Some(extension) = path.extension().and_then(|value| value.to_str()) {
        hint.with_extension(extension);
    }

    let meta_opts: MetadataOptions = Default::default();
    let fmt_opts: FormatOptions = Default::default();
    let probed = symphonia::default::get_probe()
        .format(&hint, mss, &fmt_opts, &meta_opts)
        .with_context(|| format!("unsupported audio format {}", path.display()))?;
    let mut format = probed.format;

    let track = format
        .tracks()
        .iter()
        .find(|track| track.codec_params.codec != CODEC_TYPE_NULL)
        .ok_or_else(|| anyhow!("no decodable audio track"))?;
    let track_id = track.id;
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .with_context(|| format!("unsupported codec {}", path.display()))?;

    let mut chromaprint = Chromaprint::new();
    let mut sample_buffer: Option<SampleBuffer<i16>> = None;
    let mut max_samples: Option<usize> = None;
    let mut fed_samples = 0usize;
    let mut started = false;
    let mut window_starts = Vec::new();
    let mut window_buffers = Vec::new();
    let mut window_done = Vec::new();
    let mut window_target_len = 0usize;

    loop {
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(err))
                if err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(SymphoniaError::ResetRequired) => {
                return Err(anyhow!("stream reset required"));
            }
            Err(err) => {
                return Err(anyhow!("decode packet error: {}", err));
            }
        };

        if packet.track_id() != track_id {
            continue;
        }

        while !format.metadata().is_latest() {
            format.metadata().pop();
        }

        match decoder.decode(&packet) {
            Ok(audio_buf) => {
                if sample_buffer.is_none() {
                    let spec = *audio_buf.spec();
                    let duration = audio_buf.capacity() as u64;
                    let channels = spec.channels.count();
                    if channels == 0 {
                        return Err(anyhow!("decoded stream has zero channels"));
                    }
                    let sample_rate = spec.rate;
                    if sample_rate == 0 {
                        return Err(anyhow!("decoded stream has zero sample rate"));
                    }
                    if !chromaprint.start(sample_rate as i32, channels as i32) {
                        return Err(anyhow!("failed to initialize chromaprint"));
                    }
                    started = true;
                    let total_seconds = (duration_ms / 1_000).max(1);
                    let capped_seconds = total_seconds.min(FINGERPRINT_WINDOW_SECONDS);
                    let total_samples = sample_rate as usize * channels * capped_seconds as usize;
                    max_samples = Some(total_samples);
                    window_target_len =
                        sample_rate as usize * channels * PCM_WINDOW_SECONDS as usize;
                    window_starts = target_window_starts(total_samples);
                    window_buffers = window_starts
                        .iter()
                        .map(|_| Vec::with_capacity(window_target_len))
                        .collect();
                    window_done = window_starts.iter().map(|_| false).collect();
                    sample_buffer = Some(SampleBuffer::<i16>::new(duration, spec));
                }

                if let Some(buffer) = sample_buffer.as_mut() {
                    buffer.copy_interleaved_ref(audio_buf);
                    let samples = buffer.samples();
                    let chunk_start = fed_samples;
                    let remaining = max_samples
                        .map(|limit| limit.saturating_sub(fed_samples))
                        .unwrap_or(samples.len());
                    if remaining == 0 {
                        break;
                    }
                    let take = remaining.min(samples.len());
                    for (idx, start) in window_starts.iter().enumerate() {
                        if window_done.get(idx).copied().unwrap_or(false) {
                            continue;
                        }
                        let target_end = start.saturating_add(window_target_len);
                        let overlap_start = (*start).max(chunk_start);
                        let overlap_end = target_end.min(chunk_start.saturating_add(take));
                        if overlap_start < overlap_end {
                            let local_start = overlap_start.saturating_sub(chunk_start);
                            let local_end = overlap_end.saturating_sub(chunk_start);
                            if let Some(window) = window_buffers.get_mut(idx) {
                                window.extend_from_slice(&samples[local_start..local_end]);
                                if window.len() >= target_end.saturating_sub(*start) {
                                    window_done[idx] = true;
                                }
                            }
                        } else if chunk_start > *start && chunk_start >= target_end {
                            window_done[idx] = true;
                        }
                    }
                    if take > 0 && !chromaprint.feed(&samples[..take]) {
                        return Err(anyhow!("failed feeding chromaprint"));
                    }
                    fed_samples += take;
                    if max_samples
                        .map(|limit| fed_samples >= limit)
                        .unwrap_or(false)
                    {
                        break;
                    }
                }
            }
            Err(SymphoniaError::DecodeError(_)) | Err(SymphoniaError::IoError(_)) => continue,
            Err(err) => return Err(anyhow!("decode error: {}", err)),
        }
    }

    if !started {
        return Err(anyhow!("no audio samples decoded"));
    }
    if !chromaprint.finish() {
        return Err(anyhow!("failed finalizing chromaprint"));
    }
    let fingerprint = chromaprint
        .fingerprint()
        .ok_or_else(|| anyhow!("missing chromaprint fingerprint"))?;
    let pcm_signature = hash_pcm_windows(&window_buffers);
    Ok((fingerprint, pcm_signature))
}

fn recommendation_reason(keep: &AudioEntry) -> String {
    let mut parts = Vec::new();
    if keep.lossless {
        parts.push("lossless".to_string());
    }
    if let Some(bit_depth) = keep.bit_depth {
        parts.push(format!("{}-bit", bit_depth));
    }
    if let Some(sample_rate) = keep.sample_rate {
        parts.push(format!("{:.1} kHz", sample_rate as f64 / 1000.0));
    }
    if let Some(bitrate) = keep.bitrate_kbps {
        parts.push(format!("{} kbps", bitrate));
    }
    if parts.is_empty() {
        format!("keep {}", keep.codec)
    } else {
        format!("keep {} ({})", keep.codec, parts.join(", "))
    }
}

pub(crate) fn analyze_audio_duplicates(
    files: &[(PathBuf, u64)],
    progress: Option<Arc<AudioAnalysisProgress>>,
    thread_paths: Option<Arc<Mutex<Vec<String>>>>,
    last_path: Option<Arc<Mutex<String>>>,
) -> (Vec<AudioDuplicateGroup>, Vec<String>) {
    let metadata_targets: Vec<(PathBuf, u64)> = files
        .iter()
        .filter(|(path, _)| FileType::from_path(path).is_some())
        .filter(|(path, _)| !path_contains_stem_hint(path))
        .map(|(path, size)| (path.clone(), *size))
        .collect();
    if let Some(progress) = progress.as_ref() {
        progress.set_metadata_total(metadata_targets.len());
    }

    let metadata: Vec<Result<AudioEntry>> = metadata_targets
        .par_iter()
        .map(|(path, size)| {
            let result = audio_entry(path, *size);
            if let Some(progress) = progress.as_ref() {
                progress.metadata_complete();
            }
            result
        })
        .collect();

    let mut audio_entries = Vec::new();
    let mut errors = Vec::new();
    for entry in metadata {
        match entry {
            Ok(value) => audio_entries.push(value),
            Err(err) => errors.push(err.to_string()),
        }
    }

    let mut clustered: HashMap<ClusterKey, Vec<AudioEntry>> = HashMap::new();
    for entry in audio_entries {
        let key = ClusterKey {
            duration_bucket: entry.duration_ms / DURATION_BUCKET_MS,
            channels: entry.channels.unwrap_or(0),
        };
        clustered.entry(key).or_default().push(entry);
    }

    let fingerprint_targets: Vec<AudioEntry> = clustered
        .into_values()
        .filter(|entries| entries.len() > 1)
        .flat_map(|entries| entries.into_iter())
        .collect();
    if let Some(progress) = progress.as_ref() {
        progress.set_fingerprint_total(fingerprint_targets.len());
    }

    let candidates: Vec<Result<AudioCandidate>> = fingerprint_targets
        .par_iter()
        .map(|entry| {
            if let Some(path_slot) = thread_paths.as_ref() {
                let display = entry.path.to_string_lossy().to_string();
                if let Some(last_path) = last_path.as_ref() {
                    if let Ok(mut current) = last_path.lock() {
                        *current = display.clone();
                    }
                }
                if let Ok(mut paths) = path_slot.lock() {
                    if !paths.is_empty() {
                        let idx = rayon::current_thread_index()
                            .unwrap_or(0)
                            .min(paths.len() - 1);
                        paths[idx] = display;
                    }
                }
            }
            let result = fingerprint_audio(&entry.path, entry.duration_ms).map(
                |(fingerprint, pcm_signature)| AudioCandidate {
                    entry: entry.clone(),
                    fingerprint,
                    pcm_signature,
                },
            );
            if let Some(progress) = progress.as_ref() {
                progress.fingerprint_complete();
            }
            result
        })
        .collect();

    let mut fingerprinted = Vec::new();
    for candidate in candidates {
        match candidate {
            Ok(value) => fingerprinted.push(value),
            Err(err) => errors.push(err.to_string()),
        }
    }

    let mut groups_by_fingerprint: HashMap<(String, String, u64), Vec<AudioEntry>> = HashMap::new();
    for candidate in fingerprinted {
        groups_by_fingerprint
            .entry((
                candidate.fingerprint,
                candidate.pcm_signature,
                candidate.entry.duration_ms / DURATION_BUCKET_MS,
            ))
            .or_default()
            .push(candidate.entry);
    }

    let mut groups = Vec::new();
    for ((fingerprint, _pcm_signature, _), mut entries) in groups_by_fingerprint {
        if entries.len() < 2 {
            continue;
        }
        entries.sort_by(|left, right| {
            right
                .quality_score
                .cmp(&left.quality_score)
                .then_with(|| left.path.cmp(&right.path))
        });

        let keep = entries[0].clone();
        let delete = entries
            .iter()
            .skip(1)
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>();
        let id = format!(
            "audio:{}:{}",
            keep.duration_ms,
            blake3::hash(fingerprint.as_bytes()).to_hex()
        );
        groups.push(AudioDuplicateGroup {
            id,
            fingerprint,
            duration_ms: keep.duration_ms,
            recommendation: AudioRecommendation {
                keep: keep.path.clone(),
                delete,
                reason: recommendation_reason(&keep),
            },
            entries,
        });
    }

    groups.sort_by(|left, right| {
        let left_waste: u64 = left
            .entries
            .iter()
            .filter(|entry| entry.path != left.recommendation.keep)
            .map(|entry| entry.size)
            .sum();
        let right_waste: u64 = right
            .entries
            .iter()
            .filter(|entry| entry.path != right.recommendation.keep)
            .map(|entry| entry.size)
            .sum();
        right_waste.cmp(&left_waste)
    });

    if let Some(progress) = progress.as_ref() {
        progress.finish();
    }

    (groups, errors)
}
