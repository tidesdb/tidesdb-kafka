#!/bin/bash

# Shared utility functions for TidesDB Kafka Streams scripts

# Function to print a bordered box with centered text
# Usage: print_box [color] "text line 1" "text line 2" ...
# Example: print_box "${BLUE}" "Running Tests"
# Example: print_box "" "No color text"
print_box() {
    local width=54
    local border="░"
    local color="${1:-}"
    shift
    
    # Print top border
    echo -e "${color}$(printf "${border}%.0s" $(seq 1 $width))${NC}"
    
    # Print each line of text
    for line in "$@"; do
        local text_len=${#line}
        local padding=$(( (width - text_len - 4) / 2 ))
        local right_padding=$(( width - text_len - 4 - padding ))
        printf "${color}${border}${border}%*s%s%*s${border}${border}${NC}\n" $padding "" "$line" $right_padding ""
    done
    
    # Print bottom border
    echo -e "${color}$(printf "${border}%.0s" $(seq 1 $width))${NC}"
}
