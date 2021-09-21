import csv
import os
import random

f = open('record.csv', 'w', encoding='utf-8', newline='')
csv_writer = csv.writer(f)
for i in range(1,211692):
    table = str(i)
    prefix = "../../argoverse_trajectory/forecasting_train_v1.1/train/data/"
    if os.path.exists(prefix + table + ".csv"):
        csv_reader = csv.reader(open(prefix + table + ".csv", encoding='utf-8'))
        for i,rows in enumerate(csv_reader):
            if i == 1:
                if rows[-1] == "MIA":
                    csv_writer.writerow([table])
                break
    else:
        print(table + " not found")
f.close()

f = open('sample_record.csv', 'w', encoding='utf-8', newline='')
csv_writer = csv.writer(f)
csv_reader = csv.reader(open("record.csv", encoding='utf-8'))
py = random.sample(range(1, 110421), 1000)
for i,rows in enumerate(csv_reader):
    if i in py:
        csv_writer.writerow([rows[0]])
        print(i)
f.close()
