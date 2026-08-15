from pybaseball import statcast
from pybaseball import playerid_reverse_lookup
from pybaseball import cache
import matplotlib.pyplot as plt
import seaborn as sns

cache.enable()

# same start and end date = one day
df = statcast(start_dt='2026-08-01', end_dt='2026-08-01')

#data set sanity check
##print(df.shape)
#print(list(df.columns))
uniq_description = df['description'].unique()
uniq_description_counts = df['description'].value_counts()
##print(uniq_description)
#print(uniq_description_counts)
### blocked_ball, pitchout are pitch results where the catcher ends up receiving the baseball however they should not be considered for the framing analysis because there is no intent of getting a strike while receiving the ball on those pitch results.


#print(df['fielder_2'].value_counts())
catcher_id = [543510]
player_info = playerid_reverse_lookup(catcher_id , key_type='mlbam')
print(player_info[['name_first', 'name_last', 'key_mlbam', 'key_fangraphs']])



#strike zone sanity check
print("strike zone top min:",df['sz_top'].min())
print("strike zone top max",df['sz_top'].max())
print("strike zone top mean",df['sz_top'].mean())

print("strike zone bottom min",df['sz_bot'].min())
print("strike zone bottom max",df['sz_bot'].max())
print("strike zone bottom mean",df['sz_bot'].mean())

#strikezone scatter plot
called_strike_ball = ["called_strike", "ball"]
strikes_and_balls = df[df["description"].isin(called_strike_ball)]

sns.scatterplot(data=strikes_and_balls, x="plate_x", y="plate_z", hue="description")
plt.savefig('called_strike_ball.png', dpi=300, bbox_inches='tight')
plt.show()


