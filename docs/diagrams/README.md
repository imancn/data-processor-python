# System Diagrams

This directory contains visual representations of the data processing framework architecture and components.

## Available Diagrams

### System Architecture
- **File**: `class_diagram.puml`
- **Description**: Complete system architecture diagram showing all components, relationships, and data flow
- **Format**: PlantUML (.puml)
- **Viewer**: Use PlantUML viewer or VS Code PlantUML extension

## How to View Diagrams

### Online
1. Copy the content of any `.puml` file
2. Visit [PlantUML Online Server](http://www.plantuml.com/plantuml/uml/)
3. Paste the content and view the rendered diagram

### VS Code
1. Install the "PlantUML" extension
2. Open any `.puml` file
3. Use `Ctrl+Shift+P` and select "PlantUML: Preview Current Diagram"

### Command Line
```bash
# Install PlantUML (requires Java)
# On Ubuntu/Debian:
sudo apt-get install plantuml

# Generate PNG from PUML
plantuml class_diagram.puml

# Generate SVG
plantuml -tsvg class_diagram.puml
```

## Diagram Conventions

- **Rectangles**: Classes and components
- **Arrows**: Dependencies and relationships
- **Colors**: Different component types
- **Notes**: Additional information and context
- **Packages**: Logical groupings

## Contributing

When adding new diagrams:
1. Use PlantUML format (.puml)
2. Follow naming conventions: `{purpose}_{type}.puml`
3. Include a description in this README
4. Ensure diagrams are up-to-date with code changes
