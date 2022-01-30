package reverse

func Reverse(str string) string {
	bytes := make([]byte, 0)
	for i:=len(str)-2;i>=0;i-=2{
		bytes=append(bytes,str[i],str[i+1])
	}
	return string(bytes)
}
