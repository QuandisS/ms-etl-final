import csv

input_file = 'Fraud.csv'  
output_file = 'transactions_v2.csv'  

with open(input_file, mode='r', newline='', encoding='utf-8') as infile, \
     open(output_file, mode='w', newline='', encoding='utf-8') as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    header = next(reader)
    new_header = ['id'] + header
    writer.writerow(new_header)

    for idx, row in enumerate(reader, start=1):
        new_row = [idx] + row
        writer.writerow(new_row)

print(f'Файл с добавленным столбцом id сохранён как {output_file}')
