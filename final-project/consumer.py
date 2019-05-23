from kafka import KafkaConsumer
from json import loads
import os

consumer = KafkaConsumer(
    'denver-crime',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     value_deserializer=lambda x: loads(x.decode('utf-8')))

folder_path = os.path.join(os.getcwd(), 'dataset-kafka')
batch_limit = 50
batch_counter = 1
batch_number = 1
batch_len = 3
try:
    for message in consumer:
        if batch_number > batch_len:
            writefile.close()
            break
        else:
            if batch_counter > batch_limit:
                batch_counter = 1
                batch_number += 1
                writefile.close()
            if batch_number > batch_len:
                writefile.close()
                break
            if batch_counter == 1:
                file_path = os.path.join(folder_path, ('model-' + str(batch_number) + '.txt'))
                writefile = open(file_path, "w", encoding="utf-8")
            message = message.value
            writefile.write(message)
            print('current batch : ' + str(batch_number) + ' current data for this batch : ' + str(batch_counter))
            batch_counter += 1
except KeyboardInterrupt:
    writefile.close()
    print('Keyboard Interrupt called by user, exiting.....')
