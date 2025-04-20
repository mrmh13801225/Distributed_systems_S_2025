package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"regexp"
	"path/filepath"
	"log"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}


func readFileContent(taskName string) ([]byte, error) {
	file, err := os.Open(taskName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func processKVsToTempFiles(kva []KeyValue, nReduce int) ([]*os.File, error) {
	tempFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for _, kv := range kva {
		redId := ihash(kv.Key) % nReduce
		if encoders[redId] == nil {
			tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-map-tmp-%d", redId))
			if err != nil {
				return nil, err
			}
			tempFiles[redId] = tempFile
			encoders[redId] = json.NewEncoder(tempFile)
		}
		if err := encoders[redId].Encode(&kv); err != nil {
			return nil, err
		}
	}

	return tempFiles, nil
}

func renameTempFiles(tempFiles []*os.File, taskID int) error {
	for i, file := range tempFiles {
		if file != nil {
			fileName := file.Name()
			if err := file.Close(); err != nil {
				return err
			}
			newName := fmt.Sprintf("mr-out-%d-%d", taskID, i)
			if err := os.Rename(fileName, newName); err != nil {
				return err
			}
		}
	}
	return nil
}

func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) error {
	content, err := readFileContent(reply.TaskName)
	if err != nil {
		return err
	}

	kva := mapf(reply.TaskName, string(content))
	sort.Sort(ByKey(kva))

	tempFiles, err := processKVsToTempFiles(kva, reply.NReduce)
	if err != nil {
		return err
	}

	if err := renameTempFiles(tempFiles, reply.TaskID); err != nil {
		return err
	}

	return nil
}

func ReadSpecificFile(targetNumber int, path string) (fileList []*os.File, err error) {
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, fileEntry := range files {
		if fileEntry.IsDir() {
			continue 
		}
		fileName := fileEntry.Name()
		if regex.MatchString(fileName) {
			filePath := filepath.Join(path, fileEntry.Name())
			file, err := os.Open(filePath)
			if err != nil {
				log.Fatalf("cannot open %v", filePath)
				for _, oFile := range fileList {
					oFile.Close()
				}
				return nil, err
			}
			fileList = append(fileList, file)
		}
	}
	return fileList, nil
}

func decodeFileToMap(file *os.File, kvs map[string][]string) error {
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break 
		}
		kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
	}
	return nil
}

func readIntermediateFiles(reduceID int, dir string) (map[string][]string, error) {
	fileList, err := ReadSpecificFile(reduceID, dir)
	if err != nil {
		return nil, err
	}

	kvs := make(map[string][]string)
	for _, file := range fileList {
		if err := decodeFileToMap(file, kvs); err != nil {
			file.Close()
			return nil, err
		}
		file.Close()
	}
	return kvs, nil
}

func processGroupedData(kvs map[string][]string, reducef func(string, []string) string, taskID int) error {
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	oname := "mr-out-" + strconv.Itoa(taskID)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, kvs[key])
		_, err := fmt.Fprintf(ofile, "%v %v\n", key, output)
		if err != nil {
			return err
		}
	}
	return nil
}

func DelFileByReduceId(targetNumber int, path string) error {
	pattern := fmt.Sprintf(`^mr-out-\d+-%d$`, targetNumber)
	regex, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue 
		}
		fileName := file.Name()
		if regex.MatchString(fileName) {
			filePath := filepath.Join(path, file.Name())
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteIntermediateFiles(reduceID int, dir string) error {
	return DelFileByReduceId(reduceID, dir)
}

func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) error {
	kvs, err := readIntermediateFiles(reply.TaskID, "./")
	if err != nil {
		return err
	}

	if err := processGroupedData(kvs, reducef, reply.TaskID); err != nil {
		return err
	}

	return deleteIntermediateFiles(reply.TaskID, "./")
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallForTask() *MessageReply {
	args := MessageSend{
		MsgType: AskForTask,
	}

	reply := MessageReply{}

	err := call("Coordinator.AskForTask", &args, &reply)
	if err == nil {
		return &reply
	} else {
		return nil
	}
}

func CallForReportStatus(succesType MsgType, taskID int) error {

	args := MessageSend{
		MsgType: succesType,
		TaskID:  taskID,
	}


	err := call("Coordinator.NoticeResult", &args, nil)

	return err
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		os.Exit(-1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
