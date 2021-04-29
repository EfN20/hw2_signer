package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	inCh := make(chan interface{})
	wg := &sync.WaitGroup{}
	for _, job := range jobs {
		wg.Add(1)
		outCh := make(chan interface{})
		go work(job, inCh, outCh, wg)
		inCh = outCh
	}
	fmt.Println("[ ExecutePipeline ] before wait")
	wg.Wait()
}

func work(job job, inCh, outCh chan interface{}, wg *sync.WaitGroup) {
	job(inCh, outCh)
	close(outCh)
	wg.Done()
}

func SingleHash(inCh, outCh chan interface{}) {
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	for inputData := range inCh {
		wg.Add(1)
		go func(inputData interface{}) {
			input := strconv.Itoa(inputData.(int))
			fmt.Println("[ SingleHash input ] " + input)
			mu.Lock()
			md5Data := DataSignerMd5(input)
			mu.Unlock()
			var result, md5Result string
			crc32wg := &sync.WaitGroup{}
			crc32wg.Add(2)
			go func() {
				result = DataSignerCrc32(input)
				crc32wg.Done()
			}()
			go func() {
				md5Result = DataSignerCrc32(md5Data)
				crc32wg.Done()
			}()
			crc32wg.Wait()
			fmt.Println("[ SingleHash output ] " + result + "~" + md5Result)
			outCh <- result + "~" + md5Result
			wg.Done()
		}(inputData)
	}
	fmt.Println("[ SingleHash ] before wait")
	wg.Wait()
}

func MultiHash(inCh, outCh chan interface{}) {
	wgMultiHash := &sync.WaitGroup{}
	for inputData := range inCh {
		fmt.Println("[ MultiHash input ] " + inputData.(string))
		wgMultiHash.Add(1)
		go func(inputData interface{}) {
			wg := &sync.WaitGroup{}
			mu := &sync.Mutex{}
			result_slice := make([]string, 6)
			for i := 0; i < 6; i++ {
				wg.Add(1)
				data := strconv.Itoa(i) + inputData.(string)
				go func(i int) {
					crc32Result := DataSignerCrc32(data)
					mu.Lock()
					result_slice[i] = crc32Result
					mu.Unlock()
					wg.Done()
				}(i)
			}
			wg.Wait()
			var result string
			for i := 0; i < 6; i++ {
				result += result_slice[i]
			}
			fmt.Println("[ MultiHash output ] " + result)
			outCh <- result
			wgMultiHash.Done()
		}(inputData)
	}
	fmt.Println("[ MultiHash ] before wait")
	wgMultiHash.Wait()
}

func CombineResults(inCh, outCh chan interface{}) {
	var result_slice []string
	for inputData := range inCh {
		fmt.Println("[ CombineResult ] " + inputData.(string))
		result_slice = append(result_slice, inputData.(string))
	}
	sort.Strings(result_slice)
	result := strings.Join(result_slice, "_")
	outCh <- result
}
