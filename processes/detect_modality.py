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
        if not np.isfinite(row).all():
            continue  # frequency bin with missing directional data
        if np.sum(row) < 1e-6:
            continue  # skip empty or very low energy bins

        # direction is circular, so wrap the smoothing kernel instead of
        # reflecting it at 0/355 deg
        smoothed = gaussian_filter1d(row, sigma=direction_smoothing, mode='wrap')

        # find_peaks never reports an extremum at index 0 or n-1 because those have
        # only one neighbour, so a spectrum peaking at due north (bin 0) was silently
        # dropped. Tile the row three times and keep the peaks of the middle copy, so
        # every bin has real neighbours on both sides.
        tiled = np.tile(smoothed, 3)
        peaks, props = find_peaks(tiled, prominence=min_peak_prominence * np.max(smoothed))
        peaks = np.unique(peaks[(peaks >= n_dir_bins) & (peaks < 2 * n_dir_bins)] - n_dir_bins)

        if len(peaks) >= 2:
            # Check for peak separation around the circle, including the wrap from
            # the last peak back to the first. Taking the raw difference between
            # consecutive sorted peaks called peaks at bins 1 and 70 of 72 "69 bins
            # apart" when they are really 3 bins (15 deg) apart, over-reporting
            # bimodality for any peak pair straddling north.
            sorted_peaks = np.sort(peaks)
            gaps = np.diff(sorted_peaks)
            gaps = np.append(gaps, sorted_peaks[0] + n_dir_bins - sorted_peaks[-1])
            sep = np.minimum(gaps, n_dir_bins - gaps)
            if np.any(sep >= min_peak_separation_bins):
                bins_with_multipeaks += 1
    
    # return undetermined if the wave energy is low
    if not np.isfinite(S).any() or np.nansum(S) < 1e-3:
        return "undetermined"
    elif bins_with_multipeaks >= min_bins_with_peaks:
        return "bimodal"
    else:
        return "unimodal"