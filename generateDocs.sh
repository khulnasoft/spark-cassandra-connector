#!/bin/bash

SCC_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
OUTPUT="/tmp/SCC_DOC_TEMP"

# Check for at least one version argument
if [ "$#" -lt 1 ]; then
    echo "At least one version parameter is required, e.g. 3.0.1 (no leading 'v')."
    exit 1
fi

# Check if OUTPUT directory exists and remove it if it does
if [ -d "$OUTPUT" ]; then
    rm -r "$OUTPUT"
fi
mkdir -p "$OUTPUT"
echo "SPARK CASSANDRA CONNECTOR HOME IS $SCC_HOME"

# Generate documentation for each specified version
for VERSION in "$@"; do
    echo "Making docs for $VERSION"
    git checkout "v$VERSION"
    if [ $? -ne 0 ]; then
        echo "Unable to checkout version $VERSION, skipping"
        continue
    fi

    sbt clean
    sbt doc
    mkdir -p "$OUTPUT/$VERSION"

    # Copy the documentation for each module
    for MODULE in connector driver test-support; do
        FOLDER="$SCC_HOME/$MODULE"
        if [ -d "$FOLDER/target/scala-2.12/api" ]; then
            echo "COPYING $FOLDER to $OUTPUT/$VERSION/$MODULE"
            mkdir -p "$OUTPUT/$VERSION/$MODULE"
            cp -vr "$FOLDER/target/scala-2.12/api" "$OUTPUT/$VERSION/$MODULE"
        else
            echo "No documentation found for module $MODULE in version $VERSION"
        fi
    done
done

# Check out or create the gh-pages branch
if git rev-parse --verify gh-pages >/dev/null 2>&1; then
    git checkout gh-pages
else
    echo "gh-pages branch does not exist. Creating it now."
    git checkout --orphan gh-pages
    git rm -rf .
    touch .nojekyll  # Prevents GitHub Pages from processing files as Jekyll
    git add .nojekyll
    git commit -m "Initialize gh-pages branch"
    git push origin gh-pages
fi

# Copy generated docs to ApiDocs directory
mkdir -p "$SCC_HOME/ApiDocs"
cp -r "$OUTPUT/"* "$SCC_HOME/ApiDocs"

echo "Documentation generation completed successfully."
