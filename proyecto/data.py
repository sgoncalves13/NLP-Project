import asyncio
from twscrape import API, gather
from twscrape.logger import set_log_level
import csv
from asyncio.locks import Lock

file_lock = Lock()

async def main():
    api = API()
    
    '''
    De manera asíncrona crea un regstro en una base de datos local con cada uno de los usuarios propuestos
    de esta manera accede a esta bd cada vez que intente extraer información de la red social.
    '''

    await api.pool.add_account("JuanCam9501", "Password", "juacostar95@gmail.com", "Password")
    await api.pool.add_account("JuanAco02430338", "Password", "juacostar@unal.edu.co", "Password")
    await api.pool.add_account("JuanAco06157642", "Password", "jcelmejor95@hotmail.com", "Password")
    await api.pool.add_account("JuanCamilo69214", "Password", "jc.acosta2@uniandes.edu.co", "Password")
    await api.pool.add_account("juan1414807", "Password", "juancamiloacostarojas9@gmail.com ", "Password")
    await api.pool.add_account("JuanRoj97330071", "Password", "juacostar241206@gmail.com", "Password")
    await api.pool.add_account("JuanRoj21636851", "Password", "juancamilorojas530@gmail.com", "Password")
    await api.pool.login_all()


    user_id = 28588050
    user_name = 'proscojoncio' # Usuario de donde se va a extraer el contenido humrístico
    real = True
    csv_file = 'final_data.csv'
    
    

    ''' Solicitud de manera asíncrona de diferentes post en diferentes intervalos de tiempo
     Cada tweet extraído contiene un formato json bastante extenso con bastantes atributos de este, por lo
     que solo se extraen los que contienen el texto plano de cada uno de estos
    '''
    async for tweet in api.search(f"from:{user_name} since:2013-06-01 until:2013-12-31", limit=300000000000000000):
        data = {
            'site': user_name,
            'retweetCount': tweet.retweetCount,
            'quoteCount': tweet.quoteCount,
            'likeCount': tweet.likeCount,
            'replyCount': tweet.replyCount,
            'rawContent': tweet.rawContent,
            'retweetedTweet': 'true' if tweet.retweetedTweet else 'false',
            'quotedTweet': 'true' if tweet.quotedTweet else 'false',
            'real': 1 if real else 0
        }
        await save_data(data, csv_file)
        print(tweet.id)

    async for tweet in api.search(f"from:{user_name} since:2014-01-01 until:2014-06-01", limit=300000000000000000):
        data = {
            'site': user_name,
            'retweetCount': tweet.retweetCount,
            'quoteCount': tweet.quoteCount,
            'likeCount': tweet.likeCount,
            'replyCount': tweet.replyCount,
            'rawContent': tweet.rawContent,
            'retweetedTweet': 'true' if tweet.retweetedTweet else 'false',
            'quotedTweet': 'true' if tweet.quotedTweet else 'false',
            'real': 1 if real else 0
        }
        await save_data(data, csv_file)
        print(tweet.id)

    set_log_level("DEBUG")

async def save_data(data, csv_file):
    # Acquire the lock
    async with file_lock:
        # Append tweet data to CSV file
        with open(csv_file, 'a', newline='') as csvfile:
            fieldnames = ['site', 'retweetCount', 'quoteCount', 'likeCount', 'replyCount', 'rawContent', 'retweetedTweet', 'quotedTweet', 'real']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')

            # If the file is empty, write the header
            if csvfile.tell() == 0:
                writer.writeheader()

            # Write tweet data
            writer.writerow(data)

if __name__ == "__main__":
    asyncio.run(main())