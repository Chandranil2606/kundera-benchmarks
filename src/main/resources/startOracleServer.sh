    #!/bin/bash
    echo "Starting oracle server....."
    cd /usr/local/kv-2.0.26
    rm -rf kvroot/KunderaTests
    java -jar lib/kvstore.jar kvlite -store KunderaTests&
