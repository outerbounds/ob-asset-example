"""
Consumer Flow - Retrieves data and model assets registered by ProducerFlow.

Demonstrates:
1. get_data() - retrieves a data asset registered by another flow
2. get_model() - retrieves a model asset registered by another flow
3. Cross-flow asset consumption pattern

This is the typical pattern: one flow produces assets, another consumes them.
"""

from metaflow import step, card, current
from metaflow.cards import Markdown as MD
import json

from obproject import ProjectFlow


class ConsumerFlow(ProjectFlow):
    """Consumes data and model assets produced by ProducerFlow."""

    @card(type="blank")
    @step
    def start(self):
        """Retrieve the data asset."""
        print("Retrieving data asset...")

        try:
            self.data = self.prj.get_data("sample_data")
            print(f"get_data('sample_data') succeeded")
            print(f"  Data: {self.data}")
            self.data_success = True

            current.card.append(MD("## Data Asset Retrieved"))
            current.card.append(MD(f"```json\n{json.dumps(self.data, indent=2)}\n```"))

        except Exception as e:
            print(f"get_data('sample_data') failed: {e}")
            self.data = None
            self.data_success = False
            self.data_error = str(e)

            current.card.append(MD("## Data Asset: Failed"))
            current.card.append(MD(f"**Error:** {e}"))

        self.next(self.get_model)

    @card(type="blank")
    @step
    def get_model(self):
        """Retrieve the model asset."""
        print("Retrieving model asset...")

        try:
            self.model = self.prj.get_model("sample_model")
            print(f"get_model('sample_model') succeeded")
            print(f"  Model: {self.model}")
            self.model_success = True

            current.card.append(MD("## Model Asset Retrieved"))
            current.card.append(MD(f"```json\n{json.dumps(self.model, indent=2)}\n```"))

        except Exception as e:
            print(f"get_model('sample_model') failed: {e}")
            self.model = None
            self.model_success = False
            self.model_error = str(e)

            current.card.append(MD("## Model Asset: Failed"))
            current.card.append(MD(f"**Error:** {e}"))

        self.next(self.process)

    @step
    def process(self):
        """Process the retrieved assets."""
        print("Processing retrieved assets...")

        if self.data_success:
            print(f"Data message: {self.data.get('message', 'N/A')}")
            print(f"Data values: {self.data.get('values', [])}")

        if self.model_success:
            print(f"Model type: {self.model.get('type', 'N/A')}")
            print(f"Model accuracy: {self.model.get('accuracy', 'N/A')}")

        self.next(self.end)

    @card(type="blank")
    @step
    def end(self):
        """Summary."""
        current.card.append(MD("## Summary"))

        all_passed = self.data_success and self.model_success

        if all_passed:
            current.card.append(MD("All asset retrievals succeeded."))
        else:
            current.card.append(MD("Some asset retrievals failed:"))
            if not self.data_success:
                current.card.append(MD(f"- Data: {self.data_error}"))
            if not self.model_success:
                current.card.append(MD(f"- Model: {self.model_error}"))


if __name__ == "__main__":
    ConsumerFlow()
