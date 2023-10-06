--membuat database baru
create database db_phase2;

--membuat table segment
create table table_M3
(
    "Age" varchar,
    "Attrition" varchar,
    "BusinessTravel" varchar,
    "DailyRate" int,
    "Department" varchar,
    "DistanceFromHome" int,
    "Education" int,
    "EducationField" varchar,
    "EmployeeCount" int,
    "EmployeeNumber" varchar,
    "EnvironmentSatisfaction" int,
    "Gender" varchar,
    "HourlyRate" int,
    "JobInvolvement" int,
    "JobLevel" int,
    "JobRole" varchar,
    "JobSatisfaction" int,
    "MaritalStatus" varchar,
    "MonthlyIncome" varchar,
    "MonthlyRate" int,
    "NumCompaniesWorked" int,
    "Over18" varchar,
    "OverTime" varchar,
    "PercentSalaryHike" int,
    "PerformanceRating" int,
    "RelationshipSatisfaction" int,
    "StandardHours" int,
    "StockOptionLevel" int,
    "TotalWorkingYears" int,
    "TrainingTimesLastYear" int,
    "WorkLifeBalance" int,
    "YearsAtCompany" int,
    "YearsInCurrentRole" int,
    "YearsSinceLastPromotion" int,
    "YearsWithCurrManager" int
);

COPY table_M3("Age", "Attrition","BusinessTravel", "DailyRate", "Department", "DistanceFromHome", "Education", "EducationField", "EmployeeCount", "EmployeeNumber", "EnvironmentSatisfaction", "Gender", 
              "HourlyRate", "JobInvolvement", "JobLevel", "JobRole", "JobSatisfaction", "MaritalStatus", "MonthlyIncome", "MonthlyRate", "NumCompaniesWorked", "Over18",
              "OverTime", "PercentSalaryHike", "PerformanceRating", "RelationshipSatisfaction", "StandardHours", "StockOptionLevel", "TotalWorkingYears", "TrainingTimesLastYear", "WorkLifeBalance", 
              "YearsAtCompany", "YearsInCurrentRole", "YearsSinceLastPromotion", "YearsWithCurrManager")
FROM '/private/tmp/P2M3_Andhika-Nafis_Data_Raw.csv'
DELIMITER ','
CSV HEADER;

select * from table_M3

