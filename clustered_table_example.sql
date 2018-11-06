-- how to create a clustered table in BQ, which currently requires a date partition
CREATE TABLE benchmarks.clusteredMedicare_providers(
 empty_date DATE, npi INT64, 
 nppes_provider_last_org_name STRING, 
 nppes_provider_first_name STRING,
 nppes_provider_city STRING, 
 nppes_provider_state STRING, 
 specialty_description STRING, 
 description_flag STRING,
 drug_name STRING, 
 generic_name STRING, 
 bene_count INT64, 
 total_claim_count INT64, 
 total_day_supply INT64,
 total_drug_cost FLOAT64, 
 bene_count_ge65 INT64, 
 bene_count_ge65_suppress_flag STRING, 
 total_claim_count_ge65 INT64, 
 ge65_suppress_flag STRING, 
 total_day_supply_ge65 INT64, 
 total_drug_cost_ge65 FLOAT64) 
PARTITION BY empty_date
CLUSTER BY nppes_provider_city; 

-- how to populate a clustered table from an existing table in BQ
INSERT INTO benchmarks.clusteredMedicare_providers(npi, nppes_provider_last_org_name, nppes_provider_first_name,
 nppes_provider_city, nppes_provider_state, specialty_description, description_flag, drug_name, generic_name, 
 bene_count, total_claim_count, total_day_supply, total_drug_cost, bene_count_ge65, 
 bene_count_ge65_suppress_flag, total_claim_count_ge65, ge65_suppress_flag, total_day_supply_ge65, 
 total_drug_cost_ge65)
SELECT npi, nppes_provider_last_org_name, nppes_provider_first_name,
 nppes_provider_city, nppes_provider_state, specialty_description, description_flag, drug_name, generic_name, 
 bene_count, total_claim_count, total_day_supply, total_drug_cost, bene_count_ge65, bene_count_ge65_suppress_flag, 
 total_claim_count_ge65, ge65_suppress_flag, total_day_supply_ge65, total_drug_cost_ge65 
 FROM benchmarks.medicare_providers;
