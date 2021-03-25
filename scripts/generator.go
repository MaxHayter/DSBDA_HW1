package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	highTempUpperLevel   = 500
	highTempLowerLevel   = 350
	mediumTempUpperLevel = 349
	mediumTempLowerLevel = 150
	lowTempUpperLevel    = 149
	lowTempLowerLevel    = 0

	numRows    = 10
	numColumns = 10

	widthScreen  = 1920
	heightScreen = 1080

	timeUpperLevel = "2021-03-15 19:51:23"
	timeLowerLevel = "2020-11-23 09:15:09"

	numFiles = 5

	numData = 10000

	percentBadData = 5

	numUsers = 100

	path = "./inputs"
)

func main() {
	highTempUpLvl := flag.Int("htul", highTempUpperLevel, "high temperature upper level")
	highTempLowLvl := flag.Int("htll", highTempLowerLevel, "high temperature lower level")
	mediumTempUpLvl := flag.Int("mtul", mediumTempUpperLevel, "medium temperature upper level")
	mediumTempLowLvl := flag.Int("mtll", mediumTempLowerLevel, "medium temperature lower level")
	lowTempUpLvl := flag.Int("ltul", lowTempUpperLevel, "low temperature upper level")
	lowTempLowLvl := flag.Int("ltll", lowTempLowerLevel, "low temperature lower level")

	numR := flag.Int("nr", numRows, "number of vertical areas")
	numC := flag.Int("nc", numColumns, "number of horizontal areas")

	widthS := flag.Int("w", widthScreen, "screen width")
	heightS := flag.Int("h", heightScreen, "screen height")

	timeUp := flag.String("tu", timeUpperLevel, "upper time level")
	timeLow := flag.String("tl", timeLowerLevel, "lower time level")

	numF := flag.Int("nf", numFiles, "number of files")

	numD := flag.Int("nd", numData, "number of data (rows)")

	persBad := flag.Int("b", percentBadData, "percentage of broken data (in percentages from 0 to 100)")

	numU := flag.Int("u", numUsers, "number of users")

	pathDest := flag.String("path", path, "destination directory, select a special directory for files, "+
		"so before creating new files, all the old ones will be deleted")

	flag.Parse()

	if *highTempUpLvl < *highTempLowLvl || *highTempLowLvl != *mediumTempUpLvl+1 || *mediumTempUpLvl < *mediumTempLowLvl ||
		*mediumTempLowLvl != *lowTempUpLvl+1 || *lowTempUpLvl < *lowTempLowLvl || *lowTempLowLvl < 0 {
		log.Fatalln("incorrect temperature data")
	}

	if *numR < 1 || *numC < 1 {
		log.Fatalln("incorrect number of areas")
	}

	if *widthS < 100 || *heightS < 100 {
		log.Fatalln("incorrect screen resolution (minimum 100x100)")
	}

	timeU, err := time.Parse("2006-01-02 15:04:05", *timeUp)
	if err != nil {
		log.Fatalln("incorrect date format")
	}

	timeL, err := time.Parse("2006-01-02 15:04:05", *timeLow)
	if err != nil {
		log.Fatalln("incorrect date format")
	}

	if timeU.Before(timeL) {
		log.Fatalln("incorrect dates")
	}

	if *numF < 1 {
		log.Fatalln("incorrect number of files")
	}

	if *numD < 100 {
		log.Fatalln("incorrect number of data (rows) (minimum = 100)")
	}

	if *numU < 1 {
		log.Fatalln("incorrect number of users")
	}

	if *persBad < 0 || *persBad > 100 {
		log.Fatalln("incorrect percentage of broken data")
	}

	dirStat, err := os.Stat(*pathDest)
	if err != nil {
		log.Fatalln("incorrect path")
	}
	if !dirStat.IsDir() {
		log.Fatalln("no directory entered")
	}

	err = os.RemoveAll(*pathDest)
	if err != nil {
		log.Fatalln("unable to remove files from a directory")
	}

	err = os.Mkdir(*pathDest, os.FileMode(0775))
	if err != nil {
		log.Fatalln("unable to make dir")
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		err = createFileScreen(*pathDest, *widthS, *heightS, *numC, *numR)
		if err != nil {
			log.Fatalln("unable to create file screen")
		}
		wg.Done()
	}(wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		err = createFileTemperature(*pathDest, *highTempUpLvl, *highTempLowLvl, *mediumTempUpLvl, *mediumTempLowLvl,
			*lowTempUpLvl, *lowTempLowLvl)
		if err != nil {
			log.Fatalln("unable to create file temperature")
		}
		wg.Done()
	}(wg)

	err = os.Mkdir(*pathDest+string(os.PathSeparator)+"clicks", os.FileMode(0775))
	if err != nil {
		log.Fatalln("unable to make dir")
	}

	for i := 0; i < *numF; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, i int) {
			err = createFileClicks(*pathDest+string(os.PathSeparator)+"clicks", i, *numD, *numU, *widthS, *heightS, *persBad, timeL.Unix(), timeU.Unix())
			if err != nil {
				log.Fatalf("unable to create clicck file #%d\n", i)
			}
			wg.Done()
		}(wg, i)
	}

	wg.Wait()
}

func createFileScreen(path string, width, height, numColumns, numRows int) error {
	path += string(os.PathSeparator) + "screen"
	err := os.Mkdir(path, os.FileMode(0775))
	if err != nil {
		log.Fatalln("unable to make dir")
	}

	file, err := os.Create(path + string(os.PathSeparator) + "screen")
	if err != nil {
		return err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Fatalln("unable to close file")
		}
	}()

	for i, x0, x1 := 0, 0, width/numColumns; x1 <= width; i, x0, x1 = i+1, x1+1, x1+width/numColumns {
		if x1+width/numColumns > width {
			x1 = width
		}
		for j, y0, y1 := 0, 0, height/numRows; y1 <= height; j, y0, y1 = j+1, y1+1, y1+height/numRows {
			if y1+height/numRows > height {
				y1 = height
			}
			_, err = file.WriteString(fmt.Sprintf("%d:%d;%d:%d-(%d , %d)\n", x0, y0, x1, y1, i, j))
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func createFileTemperature(path string, hu, hl, mu, ml, lu, ll int) error {
	path += string(os.PathSeparator) + "temperature"
	err := os.Mkdir(path, os.FileMode(0775))
	if err != nil {
		log.Fatalln("unable to make dir")
	}

	file, err := os.Create(path + string(os.PathSeparator) + "temperature")
	if err != nil {
		return err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Fatalln("unable to close file")
		}
	}()

	_, err = file.WriteString(fmt.Sprintf("%d-%d : %s\n", ll, lu, "low"))
	if err != nil {
		return err
	}

	_, err = file.WriteString(fmt.Sprintf("%d-%d : %s\n", ml, mu, "medium"))
	if err != nil {
		return err
	}

	_, err = file.WriteString(fmt.Sprintf("%d-%d : %s\n", hl, hu, "hot"))
	if err != nil {
		return err
	}

	return nil
}

func createFileClicks(path string, numFile, numRows, numUsers, wight, height, badData int, lowTime, upTime int64) error {
	file, err := os.Create(fmt.Sprintf("%s%s%s%d", path, string(os.PathSeparator), "clicks", numFile))
	if err != nil {
		return err
	}
	defer func() {
		err = file.Close()
		if err != nil {
			log.Fatalln("unable to close file")
		}
	}()

	for i := 0; i < numRows; i++ {
		userId := rand.Intn(numUsers) + 1
		x, y := rand.Intn(wight), rand.Intn(height)
		tim := time.Unix(rand.Int63n(upTime-lowTime)+lowTime, 0).Format("2006-01-02 15:04:05")

		if rand.Intn(100)/badData > 0 {
			_, err = file.WriteString(fmt.Sprintf("%d,%d %d - %s\n", x, y, userId, tim))
			if err != nil {
				return err
			}
		} else {
			_, err = file.WriteString(fmt.Sprintf("%d,,%s - %s\n", rand.Int(),
				base64.StdEncoding.EncodeToString([]byte(string(rand.Int31n(100)))), time.Unix(rand.Int63(), 0).String()))
			if err != nil {
				return err
			}
		}
	}

	return nil
}
