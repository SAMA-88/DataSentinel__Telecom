
-- this folder for fixed data is 
INSERT INTO crm.plans (plan_name, monthly_fee, data_gb, voice_minutes) VALUES
    -- Prepaid / entry
    ('Starter 1GB',        5.00,    1,    50),
    ('Starter 3GB',        8.00,    3,   100),
    ('Basic',             15.00,    5,   200),
    ('Basic Plus',        18.00,   10,   300),

    -- Standard postgpaid
    ('Plus',              25.00,   20,   500),
    ('Plus Max',          30.00,   40,   750),
    ('Pro',               40.00,  100,  1000),
    ('Pro Max',           50.00,  200,  2000),
    ('Unlimited',         60.00,  500,  5000),
    ('Unlimited Premium', 75.00, 1000, 10000),

    -- Family
    ('Family 2 Lines',    45.00,  100,  2000),
    ('Family 3 Lines',    60.00,  150,  3000),
    ('Family 4 Lines',    75.00,  200,  4000),

    -- Business
    ('Business Basic',    35.00,   50,  1000),
    ('Business Pro',      55.00,  200,  3000),
    ('Business Unlimited',85.00, 1000, 10000),

    -- Data only / youth / senior
    ('Data Only 50GB',    20.00,   50,     0),
    ('Data Only 200GB',   35.00,  200,     0),
    ('Youth',             12.00,   15,   300),
    ('Senior',            10.00,    3,   500);