# Rambase

This project is intented to integrate rambase historical data gathering into CI/CD pipelines with Databricks using Databricks Asset Bundles
The project was generated using the default-python template. The Databricks Asset Bundle config files were then altered to be more in line with previous asset bundle configuration.  

Daily data is ingested into documents daily and competence daily from v_ documents and v competence respectively.  
```mermaid
graph LR
;
JOB1([JOB:monthly_rambase_ingestion]);
JOB1 -->JOB-TASK-monthly_competence([TASK:monthly_competence]);

JOB2([JOB:daily_rambase_ingestion]);
JOB2 -->JOB-TASK-daily_documents([TASK:daily_documents]);
```

## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For this project a a job would be deployed with the name
    `[dev yourname] daily_rambase_ingestion` to your workspace.
    You can find that job by opening your workpace and clicking on **Jobs & Pipelines**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/demo.job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html. Or read the "getting started" documentation for
   **Databricks Connect** for instructions on running the included Python code from a different IDE.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
