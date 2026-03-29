# Databricks notebook source
# MAGIC %md
# MAGIC ![databricks_academy_logo.png](../Includes/images/databricks_academy_logo.png "databricks_academy_logo.png")

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating and Managing Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC Lakeflow Spark Declarative Pipelines provides a collection of tools that allow you to orchestrate ETL pipelines with ease on Databricks.
# MAGIC
# MAGIC **Objective:** Use Databricks Lakeflow Spark Declarative Pipelines to create an ETL pipeline. The pipeline will create three tables, which will be refreshed every time the pipeline runs.

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
schema_name = "create_pipeline"
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

####################################################################################
# Print paths to root folder and source code file
path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
newpath = path.replace('02 - Creating and Managing Spark Declarative Pipelines','Pipeline Files')
rootpath = newpath
sourcefilepath = newpath + '/Pipeline - 1.py'
print(f'NOTEBOOK PATHS FOR TASKS:\n')
print(f'  * Root folder path: \n   {rootpath}\n')
print(f'  * Source file path: \n   {sourcefilepath}')
####################################################################################

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Pipeline
# MAGIC In this lesson we have starter files for you to use in your pipeline. These are located in the folder **Pipeline Files**. To create a pipeline and add existing assets to associate it with code files already available in your workspace, complete the following:
# MAGIC
# MAGIC 1. Right click **Jobs & Pipelines** in the left navigation bar and select **Open Link in New Tab**.
# MAGIC
# MAGIC 2. In **Jobs & Pipelines** select **Create** → **ETL Pipeline**.
# MAGIC
# MAGIC 3. Complete the pipeline creation page with the following: 
# MAGIC
# MAGIC     * **Name**: Name the pipeline whatever you wish
# MAGIC     * **Default catalog**: Select the **dbacademy** catalog (or a different one if you changed the default at the beginning of the lesson) 
# MAGIC     * **Default schema**: Select the **create_pipeline** schema (or a different one if you changed the default at the beginning of the lesson)
# MAGIC
# MAGIC 4. In the options, select **Add existing assets**. In the popup, complete the following:
# MAGIC
# MAGIC - **Pipeline root folder**: Select the **Pipeline Files** folder (path is in the output of the previous cell) 
# MAGIC
# MAGIC - **Source code paths**: Within the same root folder as above, select the **Pipeline - 1.py** file
# MAGIC
# MAGIC 5. Click **Add**, This will create a pipeline and associate the correct files for this demonstration.
# MAGIC
# MAGIC   Note: We will discuss the source code for this pipeline in the next lesson.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy a Pipeline
# MAGIC Let's look at how to deploy a pipeline to production.
# MAGIC
# MAGIC Complete the following steps:
# MAGIC
# MAGIC   a. Click **Settings** in the upper-right corner (this may be a gear icon, depending on your browser's zoom level)
# MAGIC
# MAGIC   b. In the **Pipeline settings** section, you can:
# MAGIC
# MAGIC   - Modify the **Pipeline name**, if desired
# MAGIC
# MAGIC   - Change the **Run as** principle. To do this, select the pencil icon next to **Run as** to modify the option. You can only change this if there are other users in your workspace.
# MAGIC
# MAGIC   - You can optionally change the executor of the pipeline to a service principal. A service principal is an identity you create in Databricks for use with automated tools, jobs, and applications.  
# MAGIC
# MAGIC   - For more information, see the [What is a service principal?](https://docs.databricks.com/aws/en/admin/users-groups/service-principals#what-is-a-service-principal) documentation.
# MAGIC
# MAGIC     - In **Pipeline mode**, ensure **Triggered** is selected so the pipeline runs on a schedule.  
# MAGIC       - Alternatively, you can choose **Continuous** mode to keep the pipeline running at all times.  
# MAGIC       - For more details, see [Triggered vs. continuous pipeline mode](https://docs.databricks.com/aws/en/dlt/pipeline-mode).
# MAGIC
# MAGIC   c. In the **Code assets** section, you can change the **Root Folder** or **Source code** files, as needed.
# MAGIC
# MAGIC   d. The **Default location for data assets** section gives you the ability to change where tables, views, etc., will be created/refreshed, by default.
# MAGIC
# MAGIC   e. In the **Compute** section, confirm that **Serverless** compute is selected.
# MAGIC
# MAGIC   f. You can add libraries or a `requirements.txt` file in the **Pipeline environment** section.
# MAGIC
# MAGIC   g. We need to add a couple of configuration parameters to the pipeline that will be used by the pipeline's source code file. Complete the following:
# MAGIC
# MAGIC   h. Scroll down to the **Configuration** section, and click **Add configuration**.
# MAGIC
# MAGIC   - Set the **key** for the first parameter to "catalog_name"
# MAGIC   - Set the **value** for this key to "dbacademy" (or a different one if you changed the default at the beginning of the lesson) 
# MAGIC   - Set the **key** for the first parameter to "schema_name"
# MAGIC   - Set the **value** for this key to "create_pipeline" (or a different one if you changed the default at the beginning of the lesson) 
# MAGIC   - Click **Save**
# MAGIC   - Close the settings by clicking the "X" in the upper-right corner
# MAGIC
# MAGIC   i. The **Tags**, **Usage**, and **Notifications** sections are not discussed in this course.
# MAGIC
# MAGIC   j. While we are here, we are going to setup this pipeline for the next lesson. Click **Edit advanced settings**.
# MAGIC
# MAGIC   - Expand **Advanced settings** at the bottom of the window.
# MAGIC
# MAGIC   - For **Channel**, you can select either **Current** or **Preview**:
# MAGIC     - **Current** – Uses the latest stable Databricks Runtime version, recommended for production.
# MAGIC     - **Preview** – Uses a more recent, potentially less stable Runtime version, ideal for testing upcoming features.
# MAGIC     - View the [Lakeflow Spark Declarative Pipelines release notes and the release upgrade process](https://docs.databricks.com/aws/en/release-notes/dlt/) documentation for more information.
# MAGIC
# MAGIC   - We can also publish the pipeline's event log to a Delta table. We will see in the next lesson that a summary of the pipeline's events is available to us in the UI, but we can get more detailed information saved, if desired.
# MAGIC
# MAGIC     - Click **Save** to save the advanced settings.
# MAGIC   k. Click the "X" in the upper-right corner to close the Pipeline settings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schedule a Pipeline
# MAGIC
# MAGIC Once your pipeline is production-ready, you may want to schedule it to run either on a time interval or continuously. For this demonstration, we’ll configure a schedule, but not actually implement it.
# MAGIC
# MAGIC Complete the following steps to schedule the pipeline:
# MAGIC
# MAGIC a. Select the **Schedule** button in the upper-right corner (might be a small calendar icon depending on your browser's zoom level).
# MAGIC
# MAGIC b. Click **Add schedule**.
# MAGIC
# MAGIC When you schedule a pipeline, you are actually creating a single task Lakeflow Job that will run the pipeline according to the schedule you set.
# MAGIC
# MAGIC c. For the job name, leave it as is.
# MAGIC
# MAGIC d. Below **Job name**, select **Advanced**.
# MAGIC
# MAGIC e. In the **Schedule** section, configure the following:
# MAGIC - Set the **Day**.
# MAGIC - Set the time to **20:00** (8:00 PM).
# MAGIC - Leave the **Timezone** as default.
# MAGIC - Select **More options**, and under **Notifications**, add your email to receive alerts for:
# MAGIC   - **Start**
# MAGIC   - **Success**
# MAGIC   - **Failure**
# MAGIC
# MAGIC f. Optionally, uncheck **Performance optimized** if you wish to save money, but increase startup time.
# MAGIC
# MAGIC g. Since we are not actually scheduling this pipeline, click **Cancel**.
# MAGIC
# MAGIC **NOTE:** You could also set the pipeline to run a few minutes after your current time to see it start through the scheduler.