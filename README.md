# Asset API Example Project

Demonstrates the full asset lifecycle using `ob-project-utils`:

- `register_data()` / `get_data()` - Data asset registration and retrieval
- `register_model()` / `get_model()` - Model asset registration and retrieval
- Cross-flow asset consumption patterns

## Project Structure

```
ob-asset-example/
├── obproject.toml              # Project configuration
├── data/
│   └── sample_data/
│       └── asset_config.toml   # Data asset definition
├── models/
│   └── sample_model/
│       └── asset_config.toml   # Model asset definition
└── flows/
    ├── producer/
    │   └── flow.py             # Registers data and model assets
    └── consumer/
        └── flow.py             # Retrieves assets from producer
```

## Flows

### ProducerFlow

Registers `sample_data` and `sample_model` assets, then verifies retrieval
within the same flow.

### ConsumerFlow

Retrieves assets registered by ProducerFlow, demonstrating the typical
pattern where one flow produces assets and another consumes them.

## Running the Example

### Local Development

```bash
python flows/producer/flow.py run
python flows/consumer/flow.py run
```

### Deployed

1. Deploy the project:
   ```bash
   obproject-deploy
   ```

2. Run ProducerFlow (registers assets):
   ```bash
   python flows/producer/flow.py --environment pypi argo-workflows create
   ```

3. Run ConsumerFlow (retrieves assets):
   ```bash
   python flows/consumer/flow.py --environment pypi argo-workflows create
   ```

## Configuration

### Reading from Production Assets

To develop locally while reading assets from a deployed branch, add to `obproject.toml`:

```toml
[dev-assets]
branch = "prod"  # Read assets from prod branch
```

This enables the pattern of validating new code against production data
while writing to an isolated branch.

## Learn More

- [Project Assets documentation](https://docs.outerbounds.com/docs/outerbounds/project-assets)
- [Asset branch resolution](https://docs.outerbounds.com/docs/outerbounds/project-assets#asset-branch-resolution)
