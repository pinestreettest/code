# db arguments - redshift
config_name = 'config.ini'
db_name = 'redshift'

# s3 arguments
s3_name = 's3_bucket'

# schemas
public_schema = 'public'
analytcs_schema = 'perpay_analytics'
dev_schema = 'users_herr'

# working tables
user_table = 'user'
brwr_table = 'borrower'
daily_brwr_table = 'ar_daily_borrower_'
brwr_bal_table = 'ar_borrower_balances_'
core_acct_bal_table = 'core_accountbalancehistory'

# datatypes
dtypes_z_brwr1 = {'borrower_id':'Integer()',
                  'user_id':'Integer()',
                  'account_id':'Integer()',
                  'email':'String()',
                  'balences_repayment_s':'Numeric()',
                  'balences_repayment_e':'Numeric()',
                  'balences_bad_s':'Numeric()',
                  'balences_bad_e':'Numeric()',
                  'balences_credit_s':'Numeric()',
                  'balences_credit_e':'Numeric()'
                 }

# tables to write to db (intermediates)
name_z_brwr1 = 'dev_z_brwr1'
