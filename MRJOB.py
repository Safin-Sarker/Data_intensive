from mrjob.job import MRJob

class ExtractColumns(MRJob):

    def mapper(self, _, line):
        data = line.strip().split(',')
        FieldID = float(data[0])
        md = float(data[1])
        east = float(data[7])
        north = float(data[8])
        yield None, (FieldID,md, east, north)

    def reducer(self, key, values):
        with open("Data500.csv", "w") as output_file:
            for data in values:
                output_file.write("{},{},{},{}\n".format(data[0], data[1], data[2],data[3]))

if _name_ == '_main_':
    ExtractColumns.run()
