# Palantir(PLTR) Stock Data Pipeline
**Table of contents**
-	[Purpose](#purpose)
-	[Technologies](#technologies)
-	[Pipeline](#pipeline)
-	[Project Reproduction](#project-reproduction)
-	[Improvements](#improvements)

## Purpose
## Technologies
## Pipeline
## Project Reproduction
## Improvements


This project has been tested with Windows 10, utilizing conda virtual environment. Please Adjust accordingly to your system.

- **Clone the repo to the desired directory**
- **Set up GCP**
	1. Register for a GCP account. Create a Project (note down the project ID).

	2. Set up GCP IAM service account:

		a. Go to IAM >>  Service accounts  >> Create service account
			
		
		b. Grant roles: Viewer, Storage Admin, Storage Object Admin, BigQuery Admin 

		c. Skip 3rd option

	3. Create and Store key to your desired directory

		a. With the service account you just created, on the right side. Click Actions (3 dots) >> manage keys >> ADD KEY (choose JSON option). Put the key to desired directory

	4. Install [gcloud CLI](https://cloud.google.com/sdk/docs/install), type  `gcloud version` in your terminal to see if cli is installed or not

	5. Initializing gcloud CLi:

		a. Set system's environmental variable

		-	Create variable name `GOOGLE_APPLICATION_CREDENTIALS`. 
		Variable value as the full path to the JSON key that you downloaded previously

			(if using Windows, start menu >> type "env", select the appeared option and click environmental variables >> click New)
		
		
		
		b. On your terminal (Use Git Bash) Run: `gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS`

		Should see a message saying that you service account is activated

	6. Set up before Using Terraform:

		a. Allow Terraform to be executed from any directory:
		-	Download terraform, put it in desired directory

		-	In your system's environmental variable, Under the variable PATH, add an absolute path to the directory that holds terraform executable


		b. Enable GCP APIs

		-	Clock on the following links, select the right project and hit enable:

			[Enable IAM APIS](https://console.cloud.google.com/apis/library/iam.googleapis.com)

			[IAMcredentials APIS](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)

	7. Terraform Configeration to help deploy GCS Bucket and BigQuery:

		a. In the cloned files, go to `variables.tf`
		At line 9, region variable change `default` into
		`default = "your-region1"`

		b. In your terminal cd to terraform directory

		c. Run: `gcloud auth application-default login`

		d. `Y` to continue, and make sure you are on the right google account when Allow access. You then should see authentication confirmed message.

		e. Run: `terraform init` (note: if gives an error message, run it one more time)

		f. Run: `terraform plan`  (input your gcp project ID that you noted from the beginning, and make sure configuration looks ok)

		g. Run: `terraform apply` (On GCP, you should see BigQuery set up, and also a new bucket in GCS)

- **Pipeline dependencies and prefect blocks set up**

	1. Set up virtual env and make sure all dependencies are installed via `requirement.txt`

		-	 Create virtual env: (I use conda to set up virtual environment, You can use your desired way and adjust accordingly)

		-	 In terminal, run  `conda create --name pltr_project python=3.9`

		-	Activate env with `conda activate pltr_project`

		-	Install dependencies:
			
			- Change directory to where requirements.txt is located.

			-	Install requirements with:
				`pip install -r requirements.txt`

	2. Prefect Block Set up:

		a. On separate terminal run: `prefect orion start`

		b. On another terminal, register gcp block with: 
		
		`prefect block register -m prefect_gcp`

		c. Create GCS block:

		-	Go to orion GUI (http://127.0.0.1:4200/): blocks >> add a block with "+">> add "GCS Bucket"

			Block Name: `pltr-gcs`

			Bucket: `your respective GCS bucket name`, in my case it is: `pltr_data_lake_de-project-pltr`

			Add GCP Credentials:

			-	 Click add, open the json key we have for the service account, then copy and paste all info in there into Service Account Info and hit "create"

			-	Select the key we just created, then press create.
					
-	**Initial historical stock data upload**

	1.	Go to [Palantir Technologies Inc. (PLTR) Stock Historical Prices & Data - Yahoo Finance](https://finance.yahoo.com/quote/PLTR/history?p=PLTR)

	2. Adjust time period from Oct 1, 2020 to today's date, and then click apply
	![change_date_apply_yahoo_stock](/images/change_date_apply_yahoo_stock.jpg)
		
	3. Go to lower right download link, right click and then select copy link
	![copy_download_link_yahoo](/images/copy_download_link_yahoo.jpg)
		
	4. Go to `initial_historical_info_etl_gcs.py`, paste the copy linked into "Put your copied link here" line 66
	
	5. Run `python initial_etl_gcs.py`

		i. Confirm to make sure that the file is in the correct bucket & directory.
		![gcs_correct_dir](/images/gcs_correct_dir.jpg)
			
	6. Go to BigQuery (make sure you are in the right project)

	7. On the dataset "pltr_stock_info", click on the 3 dot option to create a new table.

		i. `Source`: `Google Cloud Storage` >> `browse` >> (find the data we just uploaded to gcs) click on the file and select (in my case it is pltr_historic_till_2023-03-30.parquet)
			
		ii. Name the table `pltr_historical_data`, and click create table. Then should see the table appear under `pltr_stock_info` dataset
			
	
-	**Pipeline Automation set up**

	**Please pay attention to the following notes to save yourself time**

	(Note 1: The pipeline is designed to execute on weekdays only to avoid duplicate data, so testing should only take place on PST timezone weekdays)

	(Note 2: If you wish to skip waiting for 2pm PST scheduled execution, there are two options)

	Option 1:  Just execute the pipeline by running `python extract_load_to_GCS_BQ.py`, then go straight to DBT section.
	
	Option 2: Change prefect cron job equivlant to few minutes pass your time. Ex: It is currently 5pm EST, I would schedule 5:03pm, so the schedule will take place and be ready to execute 


	1.	Deploy script

	-	Run:
				`prefect deployment build ./extract_load_to_GCS_BQ.py:scrape_load_to_GCS_BQ -n "scrape yahoo schedule test"`

		This will establsh a yaml file to allow for deployment

	-	Then Run:

		`prefect deployment apply scrape_load_to_GCS_BQ-deployment.yaml`

		Then should see `scrape-load-to-GCS-BQ` under prefect GUI's deployments section.


	2.	SET Schedule in GUI:

	-	 Deployments >>>  respective name >>> three dots on the right hand side, click edit >>>
	![prefect_deployment_schedule](/images/prefect_deployment_schedule.jpg)
	-	Go to scheduling section and click edit, then set up the configuration shown in the picture (*make adjustments to cron job if you want it to execute sooner*)
	![prefect_cron](/images/prefect_cron.jpg)

				
	3.	Need an agent to pick up job in order to execute:

	-	in a seperate terminal, we will set up agent that listens to any scheduled job (At this point we have 3 terminals, one for orian server, one for executing scripts, one will be for agent which waits for schedule job to execute)
		-	`prefect agent start -q default`    (so it looks for default workqueue)

		- At this point, if 2pm PST comes around, the agent will execute the script `scrape-load-to-GCS-BQ`
		
-	**DBT Set up (manual) with transformation**
	- Run: `dbt init`, you'll be prompted for setup configuration.
		
		- Enter a name for your project: InsertYourProjectName (in my case: `pltr_stock_info`)
		- Which database would you like to use?(Should see BigQuery as the only option) `1`
		- Desired authentication method: 1 (select oauth)
		- If choose oauth:
			- Project (project ID) : your-project-id (in my case: `de-project-pltr`)
			- Dataset: `pltr_stock_info`
			- Threads (1 or more): `4`
			- Job_execution_timeout_seconds [300]: no input, presss enter to continue
			- Desired location option: 1 [US] 2[EU], depends where you live  (Note: there might be an error down the line that will be fixed with later instructions)
	
		- Run: ` gcloud auth application-default login` , which will pop up browser to authenticate (remember to select the right google account for this)

		- Navigate to pltr_stock_info folder (created by dbt) and run `dbt debug`
			- If all checks pass, run: `dbt run`, and you can go to BigQuery and see dbt established my_frist_dbt_model, my_second_dbt_model and selective_pltr_info tables.
		
			- If it throws an Error saying: `404 Not found: ProjectID:pltr_stock_info was not found in location US or EU`

				-	First, go to BigQuery, click on `pltr_stock_info` dataset, it should show the data location, in my case it is `us-west1`. Note down your location.
			
				Remember when we were configuring and selecting either US or EU?

				After picking an option, it will say Profile pltr_stock_info written to (in mycase: `C:\Users\cloud\.dbt\profiles.yml` ), find your equivalent, and go to profiles.yml
					
				Open `profiles.yml`, change the location from `US` to your location that you just noted. For me it becomes `us-west1`, and save it.
					
		
		

- **Looker Studio Set up (manual)**

		Step to create the dashboard:
		Go to looker studio, make sure you are using the correct google account
			- Click on Blank Report
			- Should see source options: Select BigQuery
			- Navigate to select the right table that we created via dbt (In my case:  selective_pltr_info), and then click add, add to report
			- Delete the pre-populated chart
		
			- Create "Historical price since going public":
				○ Add a chart (drop down) >> Time Series, Time series chart >> SETUP section (right hand side) >> Date Range Dimension = Date_recorded >> Metric = Close_Price >> Sort = Date_Recorded, Ascending.
			
			- Create "Last 5 days price points"
				○ Add a Chart (drop down) >> Line, Line Chart >> SETUP section >> Date Range Dimension = Date_recorded >> Metric = Close_Price >> Sort = Date_Recorded, Ascending >> STYLE section >> Check: Show Points, Show data labels
				
			- Create "Trade Volume Heat map with Table"
				○ Add a Chart (drop down) >> Table, Table with Heat Map >> SETUP section >> Metric = Trade_Volume, Descending

![pltr_dashboard](/images/pltr_dashboard.png)
				
			
		
		
		
			




