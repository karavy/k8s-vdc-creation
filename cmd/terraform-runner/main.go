package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	tfengine "github.com/karavy/terraform-engine"
)

func main() {
	if err := run(); err != nil {
		log.Printf("Terraform workflow step failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	workspace := envOr("WORKSPACE_DIR", "/workspace")
	templates := envOr("TEMPLATE_DIR", "/templates")
	variablesFile := envOr("VARIABLES_FILE", "/input/terraform.auto.tfvars.json")
	variablesBytes, err := os.ReadFile(variablesFile)
	if err != nil {
		return fmt.Errorf("read mapped Terraform variables: %w", err)
	}
	var variables map[string]any
	if err := json.Unmarshal(variablesBytes, &variables); err != nil || variables == nil {
		return errors.New("mapped Terraform variables must be a JSON object")
	}
	if err := os.MkdirAll(workspace, 0o750); err != nil {
		return fmt.Errorf("create Terraform workspace: %w", err)
	}
	if err := removeStaleConfiguration(workspace); err != nil {
		return err
	}
	files, err := os.ReadDir(templates)
	if err != nil {
		return fmt.Errorf("read Terraform template ConfigMap: %w", err)
	}
	if len(files) == 0 {
		return errors.New("template ConfigMap for Terraform is empty")
	}
	for _, file := range files {
		if file.IsDir() || strings.HasPrefix(file.Name(), "..") || filepath.Base(file.Name()) != file.Name() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(templates, file.Name()))
		if err != nil {
			return fmt.Errorf("read Terraform template %q: %w", file.Name(), err)
		}
		destination := filepath.Join(workspace, file.Name())
		if err := os.WriteFile(destination, contents, 0o640); err != nil {
			return fmt.Errorf("copy Terraform template %q: %w", file.Name(), err)
		}
		if strings.HasSuffix(file.Name(), ".tf.tmpl") {
			if err := tfengine.RenderTemplate(destination, variables); err != nil {
				return fmt.Errorf("render Terraform template %q: %w", file.Name(), err)
			}
		}
	}
	mainFile, err := findTerraformFile(workspace)
	if err != nil {
		return err
	}
	variablesBytes, err = json.Marshal(variables)
	if err != nil {
		return fmt.Errorf("encode Terraform variables: %w", err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "terraform.auto.tfvars.json"), variablesBytes, 0o600); err != nil {
		return fmt.Errorf("write Terraform variable file: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	terraform, err := tfengine.CreateTerraform(workspace)
	if err != nil {
		return fmt.Errorf("initialize OpenTofu: %w", err)
	}
	lockFile := filepath.Join(workspace, ".terraform.lock.hcl")
	needsInit, err := tfengine.NeedsInit(mainFile, lockFile)
	if err != nil {
		return fmt.Errorf("check provider lock: %w", err)
	}
	if needsInit {
		if err := tfengine.RunInit(terraform, ctx); err != nil {
			return fmt.Errorf("OpenTofu init failed: %w", err)
		}
	}
	if err := tfengine.RunApply(terraform, ctx); err != nil {
		return fmt.Errorf("OpenTofu plan/apply failed: %w", err)
	}
	return nil
}

func removeStaleConfiguration(workspace string) error {
	entries, err := os.ReadDir(workspace)
	if err != nil {
		return fmt.Errorf("read Terraform workspace: %w", err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".tf") || strings.HasSuffix(name, ".tf.tmpl") || name == "terraform.auto.tfvars.json" {
			if err := os.RemoveAll(filepath.Join(workspace, name)); err != nil {
				return fmt.Errorf("remove stale Terraform file %q: %w", name, err)
			}
		}
	}
	return nil
}

func findTerraformFile(workspace string) (string, error) {
	entries, err := os.ReadDir(workspace)
	if err != nil {
		return "", err
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".tf") {
			paths = append(paths, filepath.Join(workspace, entry.Name()))
		}
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		return "", errors.New("template ConfigMap for Terraform must contain at least one .tf or .tf.tmpl file")
	}
	return paths[0], nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
