package main

import (
	"log"
	"os"
	"time"

	"github.com/faiface/beep"
	"github.com/faiface/beep/mp3"
	"github.com/faiface/beep/speaker"
)

func main() {
	file, err := os.Open("../client/1mp3")
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	streamer, format, err := mp3.Decode(file)
	if err != nil {
		log.Fatal(err)
	}
	defer streamer.Close()

	err = speaker.Init(format.SampleRate, format.SampleRate.N(time.Second/10))
	if err != nil {
		log.Fatal(err)
	}

	done := make(chan bool)
	speaker.Play(beep.Seq(streamer, beep.Callback(func() {
		done <- true
	})))
	time.Sleep(time.Second)
	<-done

}
