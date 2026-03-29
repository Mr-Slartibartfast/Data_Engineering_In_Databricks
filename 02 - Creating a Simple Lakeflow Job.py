# Databricks notebook source
# MAGIC %md
# MAGIC ![databricks_academy_logo.png](../Includes/images/databricks_academy_logo.png "databricks_academy_logo.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating a Simple Lakeflow Job
# MAGIC
# MAGIC Lakeflow Jobs provides a collection of tools that allow you to schedule and orchestrate all processing tasks on Databricks.
# MAGIC
# MAGIC **Objective:** Use Databricks Lakeflow Jobs to create a two task job. The job has been separated into two notebooks for demonstration purposes:
# MAGIC - **Jobs - Task 1 - Setup - Bronze**
# MAGIC - **Jobs - Task 2 - Silver - Gold**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important: Select Environment 4
# MAGIC The cells below may not work in other environments. To choose environment 4: 
# MAGIC 1. Click the ![environment.png](../Includes/images/environment.png "environment.png") button on the right sidebar
# MAGIC 1. Open the **Environment version** dropdown
# MAGIC 1. Select **4**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this lesson.

# COMMAND ----------

####################################################################################
# Set python variables for catalog, schema, and volume names (change, if desired)
catalog_name = "dbacademy"
schema_name = "create_job"
volume_name = "myfiles"
####################################################################################

####################################################################################
# Create the catalog, schema, and volume if they don't exist already
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
####################################################################################

####################################################################################
# Creates a file called employees.csv in the specified catalog.schema.volume
import pandas as pd
data = [
    ["1111", "Kristi", "USA", "Manager"],
    ["2222", "Sophia", "Greece", "Developer"],
    ["3333", "Peter", "USA", "Developer"],
    ["4444", "Zebi", "Pakistan", "Administrator"]
]
columns = ["ID", "Firstname", "Country", "Role"] 
df = pd.DataFrame(data, columns=columns)
file_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees.csv"
df.to_csv(file_path, index=False)
################################################################################

####################################################################################
# Creates a file called employees2.csv in the specified catalog.schema.volume
data = [
    [5555, 'Alex','USA', 'Instructor'],
    [6666, 'Sanjay','India', 'Instructor']
]
columns = ["ID","Firstname", "Country", "Role"]

## Create the DataFrame
df = pd.DataFrame(data, columns=columns)

## Create the CSV file in the course Catalog.Schema.Volume
df.to_csv(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/employees2.csv", index=False)
####################################################################################

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Generate Lakeflow Job Configuration
# MAGIC
# MAGIC Configuring this lakeflow job will require parameters unique to a given user.
# MAGIC
# MAGIC Run the cell below to print out values you'll use to configure your lakeflow job in subsequent steps.

# COMMAND ----------

path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
newpath = path.replace('02 - Creating a Simple Lakeflow Job','')
task1path = newpath + 'Jobs - Task 1 - Setup - Bronze'
task2path = newpath + 'Jobs - Task 2 - Silver - Gold'

print(f'Name your LakeFlow Job: {schema_name}_Example\n')
print(f'Catalog name: {catalog_name}')
print(f'Schema name: {schema_name}\n')
print(f'NOTEBOOK PATHS FOR TASKS:\n')
print(f'  * Task 1 notebook path: \n   {task1path}\n')
print(f'  * Task 2 notebook path: \n   {task2path}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2. Configure Job with a Notebook Task
# MAGIC
# MAGIC When using the Jobs UI to orchestrate a job with multiple tasks, you'll always begin by creating a job with a single task, and can add more if required.
# MAGIC
# MAGIC Complete the following to create a Lakeflow job with two tasks using the notebooks from above (**DEWD00 - 04A-Task 1 - Setup - Bronze** and **DEWD00 - 04B-Task 2 - Silver - Gold**):
# MAGIC
# MAGIC 1. Right-click the **Jobs & Pipelines** button on the sidebar, and open the link in a new tab. This way, you can refer to these instructions, as needed.
# MAGIC
# MAGIC 2. Confirm you are in the **Jobs & Pipelines** tab.
# MAGIC
# MAGIC 3. On the right side, select **Create -> Job**.
# MAGIC
# MAGIC 4. In the top-left of the screen, enter the **Job Name** provided above to add a name for the Lakeflow job.
# MAGIC
# MAGIC 5. Configure Job parameters by clicking, **Edit parameters** on the right-hand side of the jobs UI.
# MAGIC - Set the **key** for the first parameter to "catalog_name"
# MAGIC - Set the **value** for this key to the **Catalog name** in the output of the previous cell (default is "dbacademy")
# MAGIC - Set the **key** for the first parameter to "schema_name"
# MAGIC - Set the **value** for this key to the **Schema name** in the output of the previous cell (default is "create_job")
# MAGIC - Click **Save**
# MAGIC
# MAGIC 6. Under **Add your first task**, select **Notebook**. If **Notebook** is not listed, click **+ Add another task type** and choose **Notebook** from the options.
# MAGIC
# MAGIC 7. Follow the instructions below to set up your tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Task 1
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Setup-Bronze** |
# MAGIC | Type | Ensure **Notebook** is selected. Note in the dropdown list the many different types of tasks that can be scheduled |
# MAGIC | Source | Ensure **Workspace** is selected |
# MAGIC | Path | Use the navigator to specify the **Jobs - Task 1 - Setup - Bronze** notebook. Use the path from above to help find the notebook. |
# MAGIC | Compute     | Select **Serverless** from the dropdown menu.<br>
# MAGIC | Environment and Libraries| Ensure **Default** is selected |
# MAGIC | Create | Select the **Create task** button to create the task |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Task 2
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | New task | Select **Add task** within your job. Then select **Notebook**|
# MAGIC | Task name | Enter **Silver-Gold** |
# MAGIC | Type | Choose **Notebook**. Note in the dropdown list the many different types of lakeflow jobs that can be scheduled |
# MAGIC | Source | Choose **Workspace** |
# MAGIC | Path | Use the navigator to specify the **Jobs - Task 2 - Silver - Gold** notebook. Use the path from above to help find the notebook. |
# MAGIC | Compute     | Select **Serverless** from the dropdown menu.|
# MAGIC | Depends on | Select **Setup-Bronze** |
# MAGIC | Run if dependencies | Select **All succeeded** |
# MAGIC | Environment and Libraries| Ensure **Default** is selected |
# MAGIC | Create | Select the **Create task** button to create the task |
# MAGIC
# MAGIC ##### For better performance, please enable Performance Optimized Mode in Job Details. Otherwise, it might take 6 to 8 minutes to initiate execution.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## 3. Explore Scheduling Options
# MAGIC Complete the following steps to explore the scheduling options:
# MAGIC
# MAGIC 1. On the right hand side of the Jobs UI, locate the **Schedules & Triggers** section.
# MAGIC
# MAGIC 2. Select the **Add trigger** button to explore scheduling options.
# MAGIC
# MAGIC 3. Changing the **Trigger type** from **None (Manual)** to **Scheduled** will bring up a scheduling UI.
# MAGIC
# MAGIC    - This UI provides extensive options for setting up chronological scheduling of your LakeFlow Jobs. Settings configured with the UI can also be output in cron syntax, which can be edited as needed.
# MAGIC    
# MAGIC 4. Select **Cancel** to return to Job details.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Job
# MAGIC Select **Run now** above  **Job details** to execute the job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Review Job Run
# MAGIC
# MAGIC To review the job run:
# MAGIC
# MAGIC 1. On the Job details page, select the **Runs** tab in the top-left of the screen (you should currently be on the **Tasks** tab)
# MAGIC
# MAGIC 1. Open the output details by clicking on the timestamp field under the **Start time** column
# MAGIC
# MAGIC     - If **the job is still running**, you will see the active state of the notebook with a **Status** of **`Pending`** or **`Running`** in the right side panel.
# MAGIC
# MAGIC     - If **the job has completed**, you will see the full execution of the notebook with a **Status** of **`Succeeded`** or **`Failed`** in the right side panel

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. DROP the schema

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;")