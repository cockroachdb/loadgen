// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"math/rand"
	"time"
)

const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
const numbers = "1234567890"
const aChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

var cLastTokens = [...]string{
	"BAR", "OUGHT", "ABLE", "PRI", "PRES",
	"ESE", "ANTI", "CALLY", "ATION", "EING"}

// cLoad is the value of C at load time. See 2.1.6.1.
// It's used for the non-uniform random generator.
var cLoad int

func init() {
	rand.Seed(time.Now().UnixNano())
	cLoad = rand.Intn(256)
}

func randFromAlphabet(min, max int, alphabet string) string {
	size := max
	if max-min != 0 {
		size = randInt(min, max)
	}
	if size == 0 {
		return ""
	}

	b := make([]byte, size)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

// randAString generates a random alphanumeric string of length between min and
// max inclusive. See 4.3.2.2.
func randAString(min, max int) string {
	return randFromAlphabet(min, max, aChars)
}

// randOriginalString generates a random a-string[26..50] with 10% chance of
// containing the string "ORIGINAL" somewhere in the middle of the string.
// See 4.3.3.1
func randOriginalString() string {
	if rand.Intn(9) == 0 {
		l := randInt(26, 50)
		off := randInt(0, l-8)
		return randAString(off, off) + "ORIGINAL" + randAString(l-off-8, l-off-8)
	} else {
		return randAString(26, 50)
	}
}

// randNString generates a random numeric string of length between min anx max
// inclusive. See 4.3.2.2.
func randNString(min, max int) string {
	return randFromAlphabet(min, max, numbers)
}

// randState produces a random US state. (spec just says 2 letters)
func randState() string {
	return randFromAlphabet(2, 2, letters)
}

// randZip produces a random "zip code" - a 4-digit number plus the constant
// "11111". See 4.3.2.7.
func randZip() string {
	return randNString(4, 4) + "11111"
}

// randTax produces a random tax between [0.0000..0.2000]
func randTax() float64 {
	return float64(randInt(0, 2000)) / float64(10000.0)
}

// randInt returns a number within [min, max] inclusive.
func randInt(min, max int) int {
	return rand.Intn(max-min) + min
}

// See 4.3.2.3.
func randCLastSyllables(n int) string {
	result := ""
	for i := 0; i < 3; i++ {
		result = cLastTokens[n%10] + result
		n /= 10
	}
	return result
}

// See 4.3.2.3.
func randCLast() string {
	return randCLastSyllables(((rand.Intn(256) | rand.Intn(1000)) + cLoad) % 1000)
}
