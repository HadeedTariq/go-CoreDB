package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/HadeedTariq/go-CoreDB/db/core"
)

func main() {
	fileName := "example.log"
	testMessage1 := "This is the first log message."
	testMessage2 := "Another log entry, longer this time, to demonstrate multiple writes."
	testMessageCorrupted := "This message will be intentionally corrupted to show checksum failure."

	fmt.Println("--- Writing Log Entries ---")

	// 1. Open the file for writing (create if not exists, append if it does)
	// os.O_RDWR: Open for reading and writing
	// os.O_CREATE: Create the file if it doesn't exist
	// os.O_APPEND: Append to the file when writing
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("Error opening file for writing: %v\n", err)
		return
	}
	defer file.Close()        // Ensure the file is closed when main exits
	defer os.Remove(fileName) // Clean up the file at the end

	// Write the first message
	fmt.Printf("Writing: \"%s\"\n", testMessage1)
	if err := core.LogWriter(file, []byte(testMessage1)); err != nil {
		fmt.Printf("Error writing message 1: %v\n", err)
		return
	}

	// Write the second message
	fmt.Printf("Writing: \"%s\"\n", testMessage2)
	if err := core.LogWriter(file, []byte(testMessage2)); err != nil {
		fmt.Printf("Error writing message 2: %v\n", err)
		return
	}

	fmt.Printf("Writing (corrupted): \"%s\"\n", testMessageCorrupted)
	corruptedData := []byte("This is a corrupted message, different from the original.")
	sizeCorrupted := uint32(len(testMessageCorrupted))                     // Size of original intended message
	checksumCorrupted := crc32.ChecksumIEEE([]byte("WRONG CHECKSUM DATA")) // Intentionally wrong checksum

	corruptedBuf := new(bytes.Buffer)
	binary.Write(corruptedBuf, binary.LittleEndian, sizeCorrupted)
	binary.Write(corruptedBuf, binary.LittleEndian, checksumCorrupted)
	corruptedBuf.Write(corruptedData) // Actual data being written is different

	if _, err := file.Write(corruptedBuf.Bytes()); err != nil {
		fmt.Printf("Error writing corrupted message: %v\n", err)
		return
	}

	fmt.Println("\n--- Reading Log Entries ---")

	// 2. Open the file for reading
	readFile, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error opening file for reading: %v\n", err)
		return
	}
	defer readFile.Close() // Ensure the read file is closed

	reader := io.Reader(readFile) // Use an io.Reader for more flexible reading

	for i := 1; ; i++ {
		var size uint32
		var storedChecksum uint32

		// Read size (4 bytes)
		err := binary.Read(reader, binary.LittleEndian, &size)
		if err == io.EOF {
			break // End of file
		}
		if err != nil {
			fmt.Printf("Error reading size for entry %d: %v\n", i, err)
			break
		}

		// Read checksum (4 bytes)
		err = binary.Read(reader, binary.LittleEndian, &storedChecksum)
		if err != nil {
			fmt.Printf("Error reading checksum for entry %d: %v\n", i, err)
			break
		}

		// Read data based on the size
		data := make([]byte, size)
		n, err := io.ReadFull(reader, data) // io.ReadFull ensures 'size' bytes are read
		if err != nil {
			fmt.Printf("Error reading data for entry %d (read %d of %d bytes): %v\n", i, n, size, err)
			break
		}

		// Calculate actual checksum
		calculatedChecksum := crc32.ChecksumIEEE(data)

		fmt.Printf("\nEntry %d:\n", i)
		fmt.Printf("  Stored Size: %d bytes\n", size)
		fmt.Printf("  Stored Checksum: 0x%x\n", storedChecksum)
		fmt.Printf("  Calculated Checksum: 0x%x\n", calculatedChecksum)
		fmt.Printf("  Data: \"%s\"\n", string(data))

		if storedChecksum == calculatedChecksum {
			fmt.Println("  Checksum Match: Data is intact.")
		} else {
			fmt.Println("  Checksum Mismatch: Data may be corrupted!")
		}
	}

	fmt.Println("\n--- Program Finished ---")
}
