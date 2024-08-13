FRUIT=$1
if [ "$FRUIT" == "APPLE" ]; then
    echo "You selected an apple."
elif [ "$FRUIT" == "ORANGE" ]; then
    echo "You selected an orange."
else
    echo "Unknown fruit selected."
fi