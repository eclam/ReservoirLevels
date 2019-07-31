a=2
for i in *.tif; do
new=$(printf "%04d.tif" "$a")
mv -i -- "$i" "$new"
let a=a+2
done
