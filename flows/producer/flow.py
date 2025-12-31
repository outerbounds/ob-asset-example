"""
Producer Flow - Registers data and model assets.

Demonstrates:
1. register_data() - registers a Metaflow artifact as a data asset
2. register_model() - registers a Metaflow artifact as a model asset
3. Annotations - dynamic metadata passed at registration time

The consumer flow retrieves these assets via get_data()/get_model().
"""

from metaflow import step, card, current
from metaflow.cards import Markdown as MD
import time
import json

from obproject import ProjectFlow


class ProducerFlow(ProjectFlow):
    """Produces data and model assets."""

    @card(type="blank")
    @step
    def start(self):
        """Register a data asset."""
        # Create data as an artifact
        self.sample_data = {
            "message": "Hello from ProducerFlow",
            "run_id": current.run_id,
            "timestamp": time.time(),
            "values": [1, 2, 3, 4, 5]
        }

        # Register the artifact as a data asset
        self.prj.register_data(
            "sample_data",      # asset ID (matches data/sample_data/asset_config.toml)
            "sample_data",      # artifact name (self.sample_data)
            annotations={
                "row_count": "5",
                "source": "producer_flow",
                "pathspec": current.pathspec
            }
        )

        current.card.append(MD("## Data Asset Registered"))
        current.card.append(MD(f"**Asset ID:** sample_data"))
        current.card.append(MD(f"```json\n{json.dumps(self.sample_data, indent=2)}\n```"))

        self.next(self.register_model)

    @card(type="blank")
    @step
    def register_model(self):
        """Register a model asset."""
        # Create a mock model as an artifact
        self.sample_model = {
            "type": "mock_classifier",
            "version": "1.0",
            "accuracy": 0.95,
            "created_at": time.time(),
            "hyperparams": {
                "learning_rate": 0.01,
                "epochs": 100
            }
        }

        # Register the artifact as a model asset
        self.prj.register_model(
            "sample_model",     # asset ID (matches models/sample_model/asset_config.toml)
            "sample_model",     # artifact name (self.sample_model)
            annotations={
                "accuracy": "0.95",
                "framework": "mock",
                "pathspec": current.pathspec
            }
        )

        current.card.append(MD("## Model Asset Registered"))
        current.card.append(MD(f"**Asset ID:** sample_model"))
        current.card.append(MD(f"```json\n{json.dumps(self.sample_model, indent=2)}\n```"))

        self.next(self.verify)

    @step
    def verify(self):
        """Verify assets can be retrieved within the same flow."""
        print("Verifying asset retrieval...")

        # Retrieve the data asset we just registered
        try:
            retrieved_data = self.prj.get_data("sample_data")
            print(f"get_data('sample_data') succeeded: {retrieved_data}")
            self.data_retrieval_success = True
            self.retrieved_data = retrieved_data
        except Exception as e:
            print(f"get_data('sample_data') failed: {e}")
            self.data_retrieval_success = False
            self.data_retrieval_error = str(e)

        # Retrieve the model asset we just registered
        try:
            retrieved_model = self.prj.get_model("sample_model")
            print(f"get_model('sample_model') succeeded: {retrieved_model}")
            self.model_retrieval_success = True
            self.retrieved_model = retrieved_model
        except Exception as e:
            print(f"get_model('sample_model') failed: {e}")
            self.model_retrieval_success = False
            self.model_retrieval_error = str(e)

        self.next(self.end)

    @card(type="blank")
    @step
    def end(self):
        """Summary of asset operations."""
        current.card.append(MD("## Results"))

        if self.data_retrieval_success:
            current.card.append(MD("### Data Asset: Success"))
            current.card.append(MD(f"```json\n{json.dumps(self.retrieved_data, indent=2)}\n```"))
        else:
            current.card.append(MD("### Data Asset: Failed"))
            current.card.append(MD(f"Error: `{self.data_retrieval_error}`"))

        if self.model_retrieval_success:
            current.card.append(MD("### Model Asset: Success"))
            current.card.append(MD(f"```json\n{json.dumps(self.retrieved_model, indent=2)}\n```"))
        else:
            current.card.append(MD("### Model Asset: Failed"))
            current.card.append(MD(f"Error: `{self.model_retrieval_error}`"))


if __name__ == "__main__":
    ProducerFlow()
