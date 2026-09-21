-- Create schemas for the medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant all to estate user
GRANT ALL ON SCHEMA bronze TO estate;
GRANT ALL ON SCHEMA silver TO estate;
GRANT ALL ON SCHEMA gold TO estate;