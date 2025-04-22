import pandas as pd
import os


trip_end = pd.read_csv('data/trip_end.csv')
trip_start = pd.read_csv('data/trip_start.csv')

# save head of trip_end and trip_start to csv files to output dir
trip_end.head().to_csv('output/trip_end_head.csv', index=False)
trip_start.head().to_csv('output/trip_start_head.csv', index=False)
