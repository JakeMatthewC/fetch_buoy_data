import numpy as np
from scipy.signal import find_peaks
from scipy.ndimage import gaussian_filter1d

def detect_modality_from_dmatrix(S: np.ndarray,
                                 direction_smoothing=2,
                                 min_peak_prominence=0.05,
                                 min_peak_separation_deg=30,
                                 min_bins_with_peaks=2) -> str:
    """
    Detects wave modality from directional energy matrix D(f, θ).
    
    Parameters:
        D: 2D numpy array of shape [frequency_bins, direction_bins]
        direction_smoothing: σ for Gaussian smoothing in degrees
        min_peak_prominence: minimum prominence to count a peak (relative)
        min_peak_separation_deg: minimum angular separation to count distinct peaks
        min_bins_with_peaks: how many bins must have 2+ distinct peaks to call bimodal
    
    Returns:
        'unimodal', 'bimodal', or 'undetermined'
    """
    n_freq_bins, n_dir_bins = S.shape
    bins_with_multipeaks = 0

    # Direction bin resolution in degrees
    deg_per_bin = 360 / n_dir_bins
    min_peak_separation_bins = int(min_peak_separation_deg / deg_per_bin)

    for row in S:
        if np.sum(row) < 1e-6:
            continue  # skip empty or very low energy bins
        
        smoothed = gaussian_filter1d(row, sigma=direction_smoothing)
        peaks, props = find_peaks(smoothed, prominence=min_peak_prominence * np.max(smoothed))

        if len(peaks) >= 2:
            # Check for peak separation
            sorted_peaks = np.sort(peaks)
            for i in range(len(sorted_peaks) - 1):
                sep = (sorted_peaks[i+1] - sorted_peaks[i]) % n_dir_bins
                if sep >= min_peak_separation_bins:
                    bins_with_multipeaks += 1
                    break
    
    # return undetermined if the wave energy is low
    if np.sum(S) < 1e-3:
        return "undetermined"
    elif bins_with_multipeaks >= min_bins_with_peaks:
        return "bimodal"
    else:
        return "unimodal"