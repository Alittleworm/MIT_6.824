package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	imed_kv_result := make(map[string][]string)

	for i:=0; i < nMap;i++ {
		imed_file_name := reduceName(jobName, i, reduceTask)
		imed_file, err := os.Open(imed_file_name)
		defer imed_file.Close()
		if err != nil {
			log.Fatal("reduce error in open file", err)
		}

		imed_decoder := json.NewDecoder(imed_file)
		if err != nil {
			log.Fatal("error in Decode file ", err)
		}
		for {
			var kv KeyValue
			err = imed_decoder.Decode(&kv)
			if err != nil {
				break
			}

			_, ok := imed_kv_result[kv.Key]
			if !ok {
				imed_kv_result[kv.Key] = []string{}
			}
			imed_kv_result[kv.Key] = append(imed_kv_result[kv.Key], kv.Value)
		}
	}
	var keys []string
	for key := range imed_kv_result {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out_file_name := mergeName(jobName, reduceTask)
	file, err := os.Create(out_file_name)
	defer file.Close()
	if err != nil {
		log.Fatal("error in os.Create", err)
	}
	out_file_encoder := json.NewEncoder(file)
	for _, key := range keys{
		res := reduceF(key, imed_kv_result[key])
		out_file_encoder.Encode(KeyValue{key,res})
	}

}
