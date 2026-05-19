import os
import glob
import pandas as pd

class LakeReader:
    def __init__(self, base_path="/tmp/datalake"):
        self.base_path = base_path

    def fetch_recent_records(self, sensor_type=None, limit=50):
        curated_root = os.path.join(self.base_path, "curated")
        if not os.path.exists(curated_root):
            return []

        search_pattern = os.path.join(curated_root, "**", "*.parquet")
        all_files = glob.glob(search_pattern, recursive=True)
        
        if not all_files:
            return []

        dataframes = []
        for file_path in all_files:
            try:
                df = pd.read_parquet(file_path)
                if df.empty:
                    continue
                
                if "sensor_type" not in df.columns:
                    path_parts = file_path.split(os.sep)
                    for part in path_parts:
                        if "sensor_type=" in part:
                            df["sensor_type"] = part.split("=")[1]
                            break
                
                if sensor_type and "sensor_type" in df.columns:
                    df = df[df["sensor_type"] == sensor_type]
                
                if not df.empty:
                    dataframes.append(df)
            except Exception:
                continue

        if not dataframes:
            return []

        combined_df = pd.concat(dataframes, ignore_index=True)
        
        if "event_time" in combined_df.columns:
            combined_df = combined_df.sort_values(by="event_time", ascending=False)
        elif "timestamp" in combined_df.columns:
            combined_df = combined_df.sort_values(by="timestamp", ascending=False)

        result_df = combined_df.head(int(limit))
        
        if "event_time" in result_df.columns:
            result_df["event_time"] = result_df["event_time"].astype(str)

        return result_df.to_dict(orient="records")

    def fetch_summary_aggregates(self, sensor_type=None):
        consumption_root = os.path.join(self.base_path, "consumption")
        if not os.path.exists(consumption_root):
            return []

        search_pattern = os.path.join(consumption_root, "**", "*.parquet")
        all_files = glob.glob(search_pattern, recursive=True)
        
        if not all_files:
            return []

        dataframes = []
        for file_path in all_files:
            try:
                df = pd.read_parquet(file_path)
                if df.empty:
                    continue
                
                if "sensor_type" not in df.columns:
                    path_parts = file_path.split(os.sep)
                    for part in path_parts:
                        if "sensor_type=" in part:
                            df["sensor_type"] = part.split("=")[1]
                            break
                
                if sensor_type and "sensor_type" in df.columns:
                    df = df[df["sensor_type"] == sensor_type]
                
                if not df.empty:
                    dataframes.append(df)
            except Exception:
                continue

        if not dataframes:
            return []

        combined_df = pd.concat(dataframes, ignore_index=True)
        
        if "window" in combined_df.columns:
            combined_df["window"] = combined_df["window"].astype(str)

        return combined_df.to_dict(orient="records")