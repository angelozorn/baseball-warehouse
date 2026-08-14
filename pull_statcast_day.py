from pybaseball import statcast

# same start and end date = one day
df = statcast(start_dt='2026-08-01', end_dt='2026-08-01')

print(df.shape)
print(list(df.columns))
#print(df[['player_name', 'pitch_type', 'release_speed', 'events']].head())