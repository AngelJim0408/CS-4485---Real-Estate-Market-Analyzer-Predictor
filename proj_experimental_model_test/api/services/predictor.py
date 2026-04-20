"""
Model loading and prediction service.
Loads the saved Random Forest .joblib models and runs inference
against the latest feature data from the database.
"""

import joblib
import pandas as pd
import numpy as np
from pathlib import Path


class ModelManager:
    """Loads and manages trained Random Forest models."""

    def __init__(self):
        self.models: dict = {}  # key: target name, value: sklearn model
        self.models_path = Path("saved_models")

    def load_models(self):
        """Load all saved .joblib models from saved_models/."""
        if not self.models_path.exists():
            print("[Models] saved_models/ directory not found. Predictions disabled.")
            return

        target_names = ["target_zhvi_3m","target_zhvi_6m"]

        for target in target_names:
            model_file = self.models_path / f"{target}_rf_model.joblib"
            if model_file.exists():
                self.models[target] = joblib.load(model_file)
                print(f"[Models] Loaded {target}")
            else:
                print(f"[Models] Not found: {model_file}")

        print(f"[Models] {len(self.models)} models loaded.")

    def is_loaded(self) -> bool:
        return len(self.models) > 0

    def get_feature_names(self, target: str) -> list[str] | None:
        """Get the feature names the model was trained on."""
        model = self.models.get(target)
        if model is None:
            return None
        # sklearn stores feature names if trained with a DataFrame
        if hasattr(model, "feature_names_in_"):
            return list(model.feature_names_in_)
        return None

    def predict(self, target: str, features_df: pd.DataFrame) -> float | None:
        """
        Run prediction for a single target.

        Args:
            target: one of target_zhvi_3m, target_zhvi_6m, target_zhvi_3m_pct, target_zhvi_6m_pct
            features_df: single-row DataFrame with the feature columns the model expects

        Returns:
            Predicted value, or None if model not available.
        """
        model = self.models.get(target)
        if model is None:
            return None

        # Align features to what the model expects
        expected = self.get_feature_names(target)
        if expected:
            # Fill missing columns with NaN (model handles it via tree splits)
            for col in expected:
                if col not in features_df.columns:
                    features_df[col] = np.nan
            features_df = features_df[expected]

        prediction = model.predict(features_df)
        return float(prediction[0])

    def predict_all(self, features_df: pd.DataFrame) -> dict:
        """
        Run all available models against the same feature row.
        Returns dict of {target_name: predicted_value}.
        """
        results = {}
        for target in self.models:
            value = self.predict(target, features_df.copy())
            if value is not None:
                results[target] = value
        return results


# Singleton — shared across all requests
model_manager = ModelManager()
