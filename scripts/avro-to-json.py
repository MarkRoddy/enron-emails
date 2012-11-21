
import sys
import json
try:
    import fastavro as avro
except ImportError:
    import avro

def main(args):
    if 3 != len(args):
        print("usage %s input output" % args[0])
        return 1
    input_file_path = args[1]
    output_file_path = args[2]
    with open(input_file_path, 'rb') as infile:
        reader = avro.reader(infile)
        schema = reader.schema

        with open(output_file_path, 'w') as of:
            for record in reader:
                as_json = json.dumps(record)
                of.write(as_json)
                of.write("\n")

if __name__ == '__main__':
    sys.exit(main(sys.argv))
