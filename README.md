# tap-indeed

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from Indeed Sponsored-Jobs-API
- Extracts the following resources:
  - Ads Stats
  - Campaigns Stats
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2022 SageData
