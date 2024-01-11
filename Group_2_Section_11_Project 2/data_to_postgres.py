from datetime import date, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from time import gmtime, strftime
import datetime  
import json
import datetime
import pandas as pd
import json
import numpy as np
from sqlalchemy.sql import text
from datetime import date, timedelta
from csv import writer,DictWriter



acc_pool = pd.read_csv("/Users/krishnasurya/Documents/2_DW_project/ls.csv")

engine = create_engine('postgresql://krishnasurya:postgres@localhost:5432/project')

acc_pool.to_sql('ls', engine ,if_exists='replace',index=False)

# code to convert date column from text to date format in psql after importing csv
# ALTER TABLE ls ALTER COLUMN "date" TYPE date
# USING "date"::date
