# pipecheck

A lightweight CLI for validating and profiling CSV/JSON data pipelines before ingestion.

---

## Installation

```bash
pip install pipecheck
```

Or install from source:

```bash
git clone https://github.com/youruser/pipecheck.git
cd pipecheck && pip install -e .
```

---

## Usage

Run a validation check against a CSV or JSON file before pushing it through your pipeline:

```bash
pipecheck validate data/customers.csv
```

Profile a JSON file to inspect field types, null rates, and value distributions:

```bash
pipecheck profile data/events.json --output report.html
```

Check against a schema definition:

```bash
pipecheck validate data/orders.csv --schema schemas/orders.yaml
```

### Example Output

```
✔  File loaded: data/customers.csv (4,200 rows)
✔  No missing required fields
⚠  Column "email" — 3.2% null values
✘  Column "signup_date" — 12 rows failed date format check

Validation complete: 1 error, 1 warning
```

---

## Supported Formats

- CSV (`.csv`)
- JSON / JSON Lines (`.json`, `.jsonl`)

---

## Contributing

Pull requests are welcome. Please open an issue first to discuss any major changes.

---

## License

This project is licensed under the [MIT License](LICENSE).