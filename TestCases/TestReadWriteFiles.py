"""
take the path of the json/excel/txt files.
write a method which can do the reading part and
returns the object of the file.

"""
import json

class FileReadWrite():
    data = None
    def __init__(self):
        pass

    def readFile(self, filePath, type):
        rData = None
        with open(filePath, "r") as f:
            if (type.__eq__("json")):
                rData = json.load(f)
            elif (type.__eq__("txt")):
                rData = f.readlines()
        return rData

    def writeFile(self, filePath, type, data):
        with open(filePath, "a") as f:
            if (type.__eq__("txt")):
                f.write(str(data) + "\n")
            elif(type.__eq__("json")):
                json.dump(data, f)

    def getData(self, filePath, type):
        rData = self.readFile(filePath, type)
        textFileWrite = "textFileWrite.txt"
        jsonFileWrite = "jsonFileWrite.json"
        if (type.__eq__("txt")):
            self.writeFile(textFileWrite, type, rData)
        elif (type.__eq__("json")):
            self.writeFile(jsonFileWrite, type, rData)

        print(rData)

if __name__=="__main__":
    jsonFilePath = "/home/fidel/Test/Django/sentimentAnalysisFinal/SentimentAnalysis/TestCases/jsonFile.json"
    textFilePath = "/home/fidel/Test/Django/sentimentAnalysisFinal/SentimentAnalysis/TestCases/textFile.txt"
    FileReadWrite().getData(jsonFilePath, "json")
