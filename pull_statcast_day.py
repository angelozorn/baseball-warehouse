from pybaseball import statcast
import matplotlib.pyplot as plt
import seaborn as sns

# same start and end date = one day
df = statcast(start_dt='2026-08-01', end_dt='2026-08-01')

#data set sanity check
##print(df.shape)
#print(list(df.columns))
uniq_description = df['description'].unique()
##print(uniq_description)
uniq_description_counts = df['description'].value_counts()

##print(df['fielder_2'].value_counts())
print(uniq_description_counts)



#strike zone sanity check
print(df['sz_top'].min())
print(df['sz_top'].max())
print(df['sz_top'].mean())

print(df['sz_bot'].min())
print(df['sz_bot'].mean())
print(df['sz_bot'].max())

#strikezone scatter plot
called_strike_ball = ["called_strike", "ball"]
strikes_and_balls = df[df["description"].isin(called_strike_ball)]

sns.scatterplot(data=strikes_and_balls, x="plate_x", y="plate_z", hue="description")
plt.show()


