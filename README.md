# *Healthcare data analysis project*

A comprehensive healthcare data analysis project using Azure, focusing on patient mortality, time flow resulting in actionable insights for improved patient care.
Data Description
Data source:  
Dataset: https://physionet.org/static/published-projects/mimiciii-demo/mimic-iii-clinical-database-demo-1.4.zip

Information regarding dataset: Overview of the MIMIC-III data | MIMIC (mit.edu)

## **Data structure:**

### **Admissions table:**
Subject_id(int), hadm_id(int), admittime(timestamp), Dishtime(timestamp), deathtime(timestamp), admission_type(string), hospital_expire_flag(int)

### ***Callout table:*** 
subject_id(int), hadm_id(int), submit_wardid(int), curr_wardid(int), discharge_wardid(int), callout_outcome(string), createtime(timestamp), updatetime(timestamp), acknowledgetime(timestamp), outcometime(timestamp)

### **Icustays table:**
subject_id(int), hadm_id(int), icustay_id(int), first_wardid(int), last_wardid(int), intime(timestamp), outtime(timestamp), los(double)

### **Transfers table:**
subject_id(int), hadm_id(int), icustay_id(int), eventtype(string), prev_wardid(int), curr_wardid(int), intime(timestamp), outtime(timestamp), los(double)


## **Illustration of the workflow of the project**

![image](https://github.com/user-attachments/assets/cc756b29-f16b-43fe-a177-d23ccdddf483)

## **Methodology**
### **Stage 1:**
Data Loading: To begin I loaded data into azure blob storage from local files. I did this by establishing the connection and ingested the mimic dataset using a simple python code.


### **Stage2:**
Set up a databricks notebook environment and mount the blob storage to the notebook.

### **Data preprocessing:**
1.	Data Cleaning: Handled the missing values and dealing with inconsistent data. Changed the abbreviated terms.

![image](https://github.com/user-attachments/assets/e8e1cf9c-32cd-4aea-ba10-1860d986536e)
 
3.	Data transformation: Adding new columns and specific data frames to deal with essential null values.
4.	Checking for outliers: Plotting a boxplot to check the range of the id values in the tables.
![image](https://github.com/user-attachments/assets/a049f098-0ea7-4426-a092-15b7675f72ec)
5.  Checking the column formats: As the data I am working with has everything to do with the times at various stages of stay I wrote a method which checks the correct timestamp format.
![image](https://github.com/user-attachments/assets/fcab7b05-0109-4af7-9b0c-350fd2409552)
6.	Checking datatypes: Checking if all the columns of the dataframe are in the correct datatype and making changes if any required.
![image](https://github.com/user-attachments/assets/7c91daf5-c99c-47ee-bba4-7e686834ec02)




### **Stage 3: The cleaned data is moved to the data lake storage to perform data analysis.**
Analysis Outputs and Their Importance
### **1. Total Length of Stay According to Admission Type**
Description:
This metric calculates the total duration of a patient’s stay in the hospital, segmented by the type of admission (emergency, elective, urgent)

Importance:
Understanding the length of stay based on admission type helps in resource planning and management. For instance, emergency admissions might require more immediate and intensive resource allocation, whereas elective admissions can be planned in advance, optimizing the hospital’s capacity and resource usage.
![image](https://github.com/user-attachments/assets/b1eebcdf-188d-4b1f-87b3-5f183f8e2ccf)

### **2. ICU Stay Duration**
Description:
This metric measures the time patients spend in the Intensive Care Unit (ICU).

Importance:
The duration of ICU stay is a critical factor in assessing the severity of a patient’s condition and the effectiveness of the treatment. It also impacts the hospital’s ICU bed availability and overall operational efficiency. Reducing ICU stay times without compromising patient care can lead to cost savings and better patient outcomes.
![image](https://github.com/user-attachments/assets/ff6b996d-c3d8-4d87-81bc-cc1d52515c1b)

### **3. Ward Utilization Rate**
Description:
Ward utilization rate is calculated by dividing the ward usage count and the total wards expressed as a percentage.

Importance:
High ward utilization rates indicate efficient use of hospital resources, whereas low utilization rates may signal underuse or inefficiencies. This metric is vital for hospital administration to balance patient care needs with available resources.
The average total hours spent in each ward and the utilization rate given as percentage is plotted.



#### **Ward utilization by finding the total hours spent in each ward**

![image](https://github.com/user-attachments/assets/32cd9154-366e-4cae-9852-21c3a929afed)

#### **Calculating the ward utilization rate and plotting as bar graph**

![image](https://github.com/user-attachments/assets/884fd76a-f845-4037-bc15-8cb47d641aa4)

### **4. Time to Respond to ICU Callout**
Description:
This metric tracks the time taken from when an ICU callout is made until the patient is admitted to the ICU.

Importance:
Quick response times to ICU callouts are crucial for patient outcomes, especially in critical cases. Monitoring and improving this metric can enhance the hospital’s emergency response capabilities and potentially save lives by ensuring timely critical care interventions.
The barchart gives the total time taken for each patient and the gantt chart gives a more detailed view of the stages in the callout phase.

![image](https://github.com/user-attachments/assets/c9d30e8d-6fdb-4120-84b4-db2254c77035)

![image](https://github.com/user-attachments/assets/9684387f-50e8-4fdb-92bf-d14205ac7885)

### **5. Time Spent in Different Stages of the Stay**
Description:
This metric analyses the duration patients spend at each stage of their hospital stay.

Importance:
Understanding how long patients spend in each stage of their stay helps in identifying bottlenecks in the hospital workflow. This information is crucial for improving patient flow, reducing waiting times, and enhancing the overall quality of care.
The time spent by a patient is indicated in the chart with the ward id represented by different colours.
I used a query system to show the gantt chart respective to the admission id given in the input.

![image](https://github.com/user-attachments/assets/25b15eab-71b3-4218-b422-58d0dafea54b)

![image](https://github.com/user-attachments/assets/252a90f0-5403-47b6-8edf-1959faeedcf1)











