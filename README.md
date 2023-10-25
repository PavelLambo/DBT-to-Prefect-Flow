# DynamicDBT-PrefectFlowGenerator

## Overview
This project aims to dynamically create Prefect flows for managing DBT models. It auto-generates Python code for Prefect flows based on the dependencies between DBT models. The generated Prefect flows can be further customized to include additional tasks.
You can make it more complex by utilizing dbt tags. And run this flow for specified tag only.

```mermaid
graph TD;
    style dbt_stg fill:#000099,stroke:#000000;
    style dbt_dv fill:#000099,stroke:#000000;
    style dbt_dm fill:#000099,stroke:#000000;
    style custom_task fill:#FF0000,stroke:#000000;

    dbt_stg-->dbt_dv;
    dbt_dv-->dbt_dm;
    custom_task-->dbt_dm;
```


Requirements
Python 3.x
Prefect
DBT
Bash (for script execution)

## Installation
Clone this repository and install the necessary Python packages:

```
git clone https://gitlab.com/PavelLambo/DynamicDBT-PrefectFlowGenerator.git
cd DynamicDBT-PrefectFlowGenerator
```

## Customization
You can customize the generated Prefect flow by modifying the code template in generate_dynamic_flow function.

## Contributing
Feel free to open issues or PRs if you have improvements or fixes.

## License
This project is licensed under the MIT License - see the LICENSE.md file for details.
