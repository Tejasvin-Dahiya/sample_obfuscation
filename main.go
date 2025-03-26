package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
)

// HashFolderName generates a SHA-256 hash for a given folder name
func HashFolderName(folderName string) string {
	hash := sha256.Sum256([]byte(folderName))
	return hex.EncodeToString(hash[:])[:16] // Use first 16 characters
}

// ObfuscatePath hashes each folder in a path separately
func ObfuscatePath(originalPath string) string {
	parts := strings.Split(originalPath, string(os.PathSeparator))
	hashedParts := make([]string, len(parts))

	for i, part := range parts {
		hashedParts[i] = HashFolderName(part)
	}

	return strings.Join(hashedParts, "/") // Convert to S3-style path
}

// UploadFile uploads a file to S3 inside the obfuscated folder structure
func UploadFile(bucketName, region, originalFolder, filePath string) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return err
	}

	s3Client := s3.New(sess)

	// Extract the file name
	fileName := filepath.Base(filePath)

	// Hash the folder structure (excluding the file name)
	relativePath, _ := filepath.Rel(originalFolder, filePath)
	obfuscatedFolderPath := ObfuscatePath(filepath.Dir(relativePath))

	// Construct the S3 object key
	objectKey := fmt.Sprintf("%s/%s", obfuscatedFolderPath, fileName)

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Upload file to S3
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Body:   file,
	})

	return err
}

// Recursively upload all files inside a folder
func UploadFolder(bucketName, region, rootFolder string) error {
	return filepath.Walk(rootFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories (only upload files)
		if info.IsDir() {
			return nil
		}

		// Upload file to hashed S3 folder
		fmt.Println("Uploading:", path, "->", ObfuscatePath(path))
		return UploadFile(bucketName, region, rootFolder, path)
	})
}

func main() {
	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Read environment variables
	bucketName := os.Getenv("S3_BUCKET_NAME")
	region := os.Getenv("AWS_REGION")

	if bucketName == "" || region == "" {
		log.Fatal("Error: S3_BUCKET_NAME and AWS_REGION must be set in the .env file")
	}

	// Get folder path from command-line arguments
	if len(os.Args) < 2 {
		log.Fatal("Usage: go run main.go <folder-path>")
	}

	folderPath := os.Args[1] // Example: "my-folder"

	// Upload entire folder
	err = UploadFolder(bucketName, region, folderPath)
	if err != nil {
		log.Fatal("Upload failed:", err)
	}

	fmt.Println("All files uploaded successfully!")
}
