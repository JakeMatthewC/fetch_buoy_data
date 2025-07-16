import os
import requests

def download_noaa_year_txt(station_id, date):
    # set local paths to save files
    save_dir = f"D:\\Buoy_work\\Raws Storage\\NOAA_Raws\\year\\{station_id}"
    txt_loc_path = f"{save_dir}\\{date}_year_{station_id}.txt"
    data_spec_loc_path = f"{save_dir}\\{date}_year_{station_id}.data_spec"
    swdir_loc_path = f"{save_dir}\\{date}_year_{station_id}.swdir"
    swdir2_loc_path = f"{save_dir}\\{date}_year_{station_id}.swdir2"
    swr1_loc_path = f"{save_dir}\\{date}_year_{station_id}.swr1"
    swr2_loc_path = f"{save_dir}\\{date}_year_{station_id}.swr2"

    # build paths to pull data from NDBC
    txt_file_id = f"{station_id}h{date}"
    txt_file_url = f"https://www.ndbc.noaa.gov/view_text_file.php?filename={txt_file_id}.txt.gz&dir=data/historical/stdmet/"

    data_spec_file_id = f"{station_id}w{date}"
    data_spec_file_url = f"https://www.ndbc.noaa.gov/view_text_file.php?filename={data_spec_file_id}.txt.gz&dir=data/historical/swden/"

    swdir_file_id = f"{station_id}d{date}"
    swdir_file_url = f"https://www.ndbc.noaa.gov/view_text_file.php?filename={swdir_file_id}.txt.gz&dir=data/historical/swdir/"

    swdir2_file_id = f"{station_id}i{date}"
    swdir2_file_url = f"https://www.ndbc.noaa.gov/view_text_file.php?filename={swdir2_file_id}.txt.gz&dir=data/historical/swdir2/"

    swr1_file_id = f"{station_id}j{date}"
    swr1_file_url = f"https://www.ndbc.noaa.gov/view_text_file.php?filename={swr1_file_id}.txt.gz&dir=data/historical/swr1/"

    swr2_file_id = f"{station_id}k{date}"
    swr2_file_url = f"https://www.ndbc.noaa.gov/view_text_file.php?filename={swr2_file_id}.txt.gz&dir=data/historical/swr2/"

    # make lists to loop through
    loc_path_list = [txt_loc_path, data_spec_loc_path, swdir_loc_path, swdir2_loc_path, swr1_loc_path, swr2_loc_path]
    url_list = [txt_file_url, data_spec_file_url, swdir_file_url, swdir2_file_url, swr1_file_url, swr2_file_url]
    id_list = [txt_file_id, data_spec_file_id, swdir_file_id, swdir2_file_id, swr1_file_id, swr2_file_id]

    os.makedirs(save_dir, exist_ok=True)

    for path,url,id in zip(loc_path_list, url_list, id_list):
        if os.path.exists(path):
            print(f"Already exists: {path}")
            continue
        
        try:
            response = requests.get(url, timeout=15)
            response.raise_for_status()

            with open(path,"w", encoding="utf-8") as f:
                f.write(response.text)

            print(f"Saved {id} to {path}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {id}")


