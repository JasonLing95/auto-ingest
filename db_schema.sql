"""
CREATE TABLE IF NOT EXISTS companies (
    company_id SERIAL PRIMARY KEY,
    cik_number VARCHAR(10) NOT NULL UNIQUE,
    company_name VARCHAR(255) NOT NULL,
    company_mail_street1 VARCHAR(255),
    company_mail_street2 VARCHAR(255),
    company_mail_city VARCHAR(100),
    company_mail_state VARCHAR(100),
    company_mail_state_desc VARCHAR(100),
    company_zipcode VARCHAR(20),
    company_business_street1 VARCHAR(255),
    company_business_street2 VARCHAR(255),
    company_business_city VARCHAR(100),
    company_business_state VARCHAR(100),
    company_business_state_desc VARCHAR(100),
    company_business_zipcode VARCHAR(20),
    company_phone VARCHAR(50),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
""",
"""
CREATE TABLE IF NOT EXISTS filings (
    filing_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(company_id),
    form_type VARCHAR(10) NOT NULL,
    accession_number VARCHAR(20) NOT NULL UNIQUE,
    filing_date DATE NOT NULL,
    file_number VARCHAR(30) NULL,
    filing_directory VARCHAR(255),
    period_of_report DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
""",
"""
CREATE TABLE IF NOT EXISTS issuers (
    issuer_id SERIAL PRIMARY KEY,
    cusip VARCHAR(9) UNIQUE,
    issuer_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
""",
"""
CREATE TABLE IF NOT EXISTS title_of_class_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
)
""",
"""
CREATE TABLE IF NOT EXISTS share_type_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
)
""",
"""
CREATE TABLE IF NOT EXISTS put_or_call_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
)
""",
"""
CREATE TABLE IF NOT EXISTS investment_discretion_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
)
""",
"""
CREATE TABLE IF NOT EXISTS holdings (
    holding_id BIGSERIAL PRIMARY KEY,
    filing_id INTEGER REFERENCES filings(filing_id) ON DELETE CASCADE,
    issuer_id INTEGER REFERENCES issuers(issuer_id),
    title_of_class INTEGER REFERENCES title_of_class_table(id),
    shares_or_principal_amount BIGINT NOT NULL,
    shares_or_principal_type INTEGER REFERENCES share_type_table(id),
    value BIGINT NOT NULL,
    put_or_call INTEGER REFERENCES put_or_call_table(id),
    investment_discretion INTEGER REFERENCES investment_discretion_table(id),
    voting_authority_sole BIGINT,
    voting_authority_shared BIGINT,
    voting_authority_none BIGINT
)
"""