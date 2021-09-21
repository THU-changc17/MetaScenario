import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

behavior_interaction_arr = np.zeros((6, 11)).astype(np.int)

# HighD
behavior_interaction_arr[0] = np.array([2921, 3431, 1310, 210, 9, 0, 0, 0, 0, 0, 0])
behavior_interaction_arr[1] = np.array([70, 72, 23, 1, 0, 0, 0, 0, 0, 0, 0])
behavior_interaction_arr[2] = np.array([83, 93, 15, 1, 0, 0, 0, 0, 0, 0, 0])


# NGSIM
# behavior_interaction_arr[0] = np.array([865, 3451, 5689, 6570, 5794, 4057, 2490, 1446, 766, 369, 273])
# behavior_interaction_arr[1] = np.array([50, 144, 166, 63, 16, 7, 0, 0, 0, 0, 0])
# behavior_interaction_arr[2] = np.array([31, 122, 252, 285, 225, 149, 75, 37, 18, 9, 3])
# behavior_interaction_arr[3] = np.array([13, 26, 69, 80, 59, 26, 20, 12, 8, 5, 0])

# Merge
# behavior_interaction_arr[0] = np.array([1236, 3276, 3863, 4109, 4895, 4225, 2782, 2037, 1113, 339, 100])
# behavior_interaction_arr[1] = np.array([2, 0, 4, 6, 40, 30, 22, 2, 9, 0, 0])
# behavior_interaction_arr[2] = np.array([86, 115, 85, 154, 347, 404, 188, 60, 15, 0, 0])
# behavior_interaction_arr[3] = np.array([70, 70, 38, 53, 39, 67, 51, 32, 14, 6, 0])
# behavior_interaction_arr[4] = np.array([74, 43, 18, 19, 22, 18, 13, 5, 2, 0, 1])

# Intersection
# behavior_interaction_arr[0] = np.array([277, 438, 474, 300, 242, 139, 62, 22, 10, 4, 1])
# behavior_interaction_arr[1] = np.array([19, 35, 47, 34, 19, 14, 9, 4, 4, 0, 0])
# behavior_interaction_arr[2] = np.array([12, 22, 33, 31, 21, 14, 10, 5, 0, 1, 0])

# Argoverse
# behavior_interaction_arr[1] = np.array([1706, 1530, 1201, 867, 740, 506, 350, 233, 205, 131, 283])
# behavior_interaction_arr[2] = np.array([107, 134, 107, 100, 72, 71, 54, 24, 26, 41, 77])
# behavior_interaction_arr[3] = np.array([77, 96, 72, 62, 48, 46, 41, 19, 15, 14, 36])
# behavior_interaction_arr[4] = np.array([55, 96, 90, 72, 86, 69, 56, 34, 33, 12, 45])
# behavior_interaction_arr[5] = np.array([31, 38, 49, 45, 32, 30, 23, 17, 21, 4, 16])
# behavior_interaction_arr[0] = np.array([10079, 7676, 5738, 4162, 3033, 2037, 1343, 778, 500, 338, 610])

# print(all_trajectory_num)
print(behavior_interaction_arr)
all_trajectory_num = behavior_interaction_arr.sum()
df1 = pd.DataFrame(behavior_interaction_arr[:3,:11],
                  index=['Go Straight', 'Left Lane Change', 'Left Right Change'],
                  columns=['Free Driving', 'V2V_1', 'V2V_2', 'V2V_3', 'V2V_4', 'V2V_5', 'V2V_6', 'V2V_7', 'V2V_8', 'V2V_9', 'V2V_10'])
# df2 = pd.DataFrame(behavior_interaction_arr[:3, :11].T,
#                   index=['Free Driving', 'V2V_1', 'V2V_2', 'V2V_3', 'V2V_4', 'V2V_5', 'V2V_6', 'V2V_7', 'V2V_8', 'V2V_9', 'V2V_10'],
#                   columns=['Go Straight', 'Turn Left', 'Turn Right'])
# df.plot.bar()
# df.plot.hist(alpha=0.5)
# sns.set_palette("pastel", 8)
plt.rcParams['figure.figsize'] = (16, 6)
df1.plot(kind='bar', alpha=0.6, fontsize=12, logy=True, rot=0)
plt.grid()
plt.ylabel('Atom Scenario Count',fontdict={'size' : 12, 'color':'darkblue'})
plt.xlabel('Behavior',fontdict={'size' : 12, 'color':'darkblue'})
plt.savefig("../statistic_highd.eps")
plt.show()
# df2.plot.barh(stacked=True, alpha=0.5)
# plt.rcParams['figure.figsize'] = (6.0, 6.0)
# # # df1.plot.box()
# df2.plot.box()
# plt.savefig("../box_ngsim.eps")
# plt.show()
# plt.show()
# df.plot.kde()
# plt.show()