import logging
import re
import sys


def sort_class_attributes(filename):
    with open(filename, "r") as f:
        lines = f.readlines()

    # check BaseDags class
    class_pattern = re.compile(r"^class\s+(\w+)\(BaseDags\):")
    attribute_pattern = re.compile(r"^\s+self\.(\w+)\s*=\s*.*")

    sorted_lines = []
    inside_class = False
    attributes = []
    class_header = None

    for line in lines:
        class_match = class_pattern.match(line)
        if class_match:
            if inside_class and attributes:
                sorted_lines += sorted(attributes, key=lambda x: x.split("=")[0].strip())
                attributes = []
            sorted_lines.append(line)
            inside_class = True
            class_header = line
            continue

        # sort attributes
        if inside_class:
            attribute_match = attribute_pattern.match(line)
            if attribute_match:
                attributes.append(line)
            else:
                if attributes:
                    sorted_lines += sorted(attributes, key=lambda x: x.split("=")[0].strip())
                    attributes = []
                sorted_lines.append(line)
                if line.strip() == "":
                    inside_class = False
        else:
            sorted_lines.append(line)

    if inside_class and attributes:
        sorted_lines += sorted(attributes, key=lambda x: x.split("=")[0].strip())

    # update file
    with open(filename, "w") as f:
        f.writelines(sorted_lines)

    print(f"Sorted attributes in '{filename}'")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.warning("Usage: python sort_class_attributes.py <filename1> <filename2> ... to sort the attributes")
        sys.exit(1)

    for file in sys.argv[1:]:
        sort_class_attributes(file)
