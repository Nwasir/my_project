import unittest
import os
import pandas as pd
from src import pipeline

class TestPipeline(unittest.TestCase):
    def test_run_pipeline(self):
        # Run the pipeline
        try:
            pipeline.run_pipeline()
        except Exception as e:
            self.fail(f"run_pipeline() raised an exception: {e}")

        # Find the most recent processed file
        processed_dir = "data/processed"
        processed_files = [f for f in os.listdir(processed_dir) if f.startswith("merged_") and f.endswith(".csv")]
        self.assertTrue(processed_files, "No processed files were created.")

        latest_file = max([os.path.join(processed_dir, f) for f in processed_files], key=os.path.getctime)

        # Load and check the data
        df = pd.read_csv(latest_file)
        self.assertFalse(df.empty, "Processed dataframe is empty.")

        # Accept TMAX_F, TMAX_F_x, or TMAX_F_y
        tmaxf_cols = [col for col in df.columns if col.startswith("TMAX_F")]
        self.assertTrue(tmaxf_cols, "Missing any 'TMAX_F' column.")
        self.assertIn("TMIN_F", df.columns, "Missing 'TMIN_F' column.")
        self.assertIn("energy_usage", df.columns, "Missing 'energy_usage' column.")

if __name__ == "__main__":
    unittest.main()