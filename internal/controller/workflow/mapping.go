package workflow

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func getPointer(document any, pointer string) (any, error) {
	if pointer == "" {
		return document, nil
	}
	segments, err := pointerSegments(pointer)
	if err != nil {
		return nil, err
	}
	value := document
	for _, segment := range segments {
		switch current := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = current[segment]
			if !ok {
				return nil, fmt.Errorf("key %q does not exist", segment)
			}
		case []any:
			index, parseErr := strconv.Atoi(segment)
			if parseErr != nil || index < 0 || index >= len(current) {
				return nil, fmt.Errorf("array index %q does not exist", segment)
			}
			value = current[index]
		default:
			return nil, fmt.Errorf("cannot traverse through %T", value)
		}
	}
	return value, nil
}

func setObjectPointer(document map[string]any, pointer string, value any) error {
	segments, err := pointerSegments(pointer)
	if err != nil || len(segments) == 0 {
		return errors.New("target must be a non-root JSON pointer into an object")
	}
	current := document
	for _, segment := range segments[:len(segments)-1] {
		next, ok := current[segment]
		if !ok {
			child := make(map[string]any)
			current[segment] = child
			current = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("target parent %q is %T, not a JSON object", segment, next)
		}
		current = child
	}
	current[segments[len(segments)-1]] = value
	return nil
}

func pointerSegments(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("JSON pointer must be empty or start with '/'")
	}
	raw := strings.Split(pointer[1:], "/")
	segments := make([]string, len(raw))
	for i, part := range raw {
		var b strings.Builder
		for j := 0; j < len(part); j++ {
			if part[j] != '~' {
				b.WriteByte(part[j])
				continue
			}
			if j+1 >= len(part) {
				return nil, errors.New("invalid '~' escape in JSON pointer")
			}
			j++
			switch part[j] {
			case '0':
				b.WriteByte('~')
			case '1':
				b.WriteByte('/')
			default:
				return nil, errors.New("invalid '~' escape in JSON pointer")
			}
		}
		segments[i] = b.String()
	}
	return segments, nil
}
