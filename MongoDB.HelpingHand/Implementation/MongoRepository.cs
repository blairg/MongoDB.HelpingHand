using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.HelpingHand.Entities;

namespace MongoDB.HelpingHand.Implementation
{
    public class MongoRepository<T> : IMongoRepository<T>
    {
        public IMongoClient MongoClient { get; set; }
        public IMongoCollection<T> MongoCollection { get; set; }

        public MongoRepository(string server, string databaseName, string collectionName)
        {
            if (string.IsNullOrEmpty(server))
            {
                throw new ArgumentNullException(nameof(server));
            }

            if (string.IsNullOrEmpty(databaseName))
            {
                throw new ArgumentNullException(nameof(databaseName));
            }

            if (string.IsNullOrEmpty(collectionName))
            {
                throw new ArgumentNullException(nameof(collectionName));
            }

            MongoClient = new MongoClient("mongodb://" + server);
            var database = MongoClient.GetDatabase(databaseName);
            MongoCollection = database.GetCollection<T>(collectionName);
        }

        /// <summary>
        /// Gets all documents in the collection of type T.
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetAll()
        {
            return await GetDocuments(new BsonDocument());
        }

        /// <summary>
        /// Gets all matches which match the keys and values passed.
        /// </summary>
        /// <param name="entries"></param>
        /// <param name="operatorValue">Must be And or Or</param>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetMatches(IList<BsonDocumentBuilder> entries, Operator operatorValue = Operator.And)
        {
            CheckOperatorsAreValid(entries, operatorValue);
            FilterDefinition<T> filterDefinition = BuildFilterDefinitionWithOperator(entries, operatorValue);

            return await GetDocuments(filterDefinition);
        }
        
        /// <summary>
        /// Gets the first document by it's ObjectId.
        /// </summary>
        /// <param name="objectId"></param>
        /// <returns></returns>
        public async Task<T> GetFirst(string objectId)
        {
            ObjectId parsedObjectId = ValidateObjectId(objectId);
            return await GetDocument(new BsonDocument { { "_id", new BsonObjectId(parsedObjectId) } });
        }

        /// <summary>
        /// Get first match which matches the keys and values passed.
        /// </summary>
        /// <param name="entries"></param>
        /// <param name="operatorValue">Must be And Or Or</param>
        /// <returns></returns>
        public async Task<T> GetFirst(IList<BsonDocumentBuilder> entries, Operator operatorValue = Operator.And)
        {
            CheckOperatorsAreValid(entries, operatorValue);
            FilterDefinition<T> filterDefinition = BuildFilterDefinitionWithOperator(entries, operatorValue);

            return await GetDocument(filterDefinition);
        }

        /// <summary>
        /// Performs a regular expression search against a single column. Case be sensitive or not.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="sensitive">True for case-senstive and false (default) if not.</param>
        /// <returns>List of T</returns>
        public async Task<IEnumerable<T>> Search(string key, string value, bool sensitive = false)
        {
            var bsonDocumentBuilder = new BsonDocumentBuilder
            {
                Key = key, Value = value, Operator = Operator.Regex
            };

            FilterDefinition<T> filterDefinition = BuildFilterDefinition(bsonDocumentBuilder, sensitive);

            return await GetDocuments(filterDefinition);
        }

        /// <summary>
        /// Inserts a single document
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public async void Insert(T value)
        {
            await MongoCollection.InsertOneAsync(value);
        }

        /// <summary>
        /// Inserts a batch of documents
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public async void InsertBatch(IEnumerable<T> values)
        {
            await MongoCollection.InsertManyAsync(values);
        }

        /// <summary>
        /// Partial update of a document
        /// </summary>
        /// <param name="objectId"></param>
        /// <param name="entries">List of all fields which require updating</param>
        /// <returns></returns>
        public async Task<bool> Update(string objectId, IList<BsonDocumentBuilder> entries)
        {
            ObjectId parsedObjectId = ValidateObjectId(objectId);
            bool updated = true;

            foreach (var entry in entries)
            {
                var filter = Builders<T>.Filter.Eq("_id", parsedObjectId);
                var update = Builders<T>.Update.Set(entry.Key, entry.Value);
                var updateResult = await MongoCollection.UpdateOneAsync(filter, update);

                if (updateResult.ModifiedCount != 1)
                {
                    updated = false;
                }
            }

            return updated;
        }

        /// <summary>
        /// Full update of a document
        /// </summary>
        /// <param name="objectId"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public async Task<bool> Update(string objectId, T value)
        {
            ObjectId parsedObjectId = ValidateObjectId(objectId);
            var filter = Builders<T>.Filter.Eq("_id", parsedObjectId);
            var replaceResult = await MongoCollection.ReplaceOneAsync(filter, value);

            return replaceResult.ModifiedCount == 1;
        }

        /// <summary>
        /// Deletes all documents in the collection.
        /// </summary>
        /// <returns></returns>
        public async Task<bool> DeleteAll()
        {
            var deleteResult = await MongoCollection.DeleteManyAsync(new BsonDocument());
            return deleteResult.DeletedCount > 0;
        }

        /// <summary>
        /// Delete a document
        /// </summary>
        /// <param name="objectId"></param>
        public async Task<bool> Delete(string objectId)
        {
            ObjectId parsedObjectId = ValidateObjectId(objectId);
            var filter = Builders<T>.Filter.Eq("_id", parsedObjectId);
            var deleteResult = await MongoCollection.DeleteOneAsync(filter);

            return deleteResult.DeletedCount >= 1;
        }

        private async Task<IEnumerable<T>> GetDocuments(FilterDefinition<T> filterDefinition)
        {
            IList<T> listToReturn = new List<T>();

            using (var cursor = await MongoCollection.FindAsync(filterDefinition))
            {
                while (await cursor.MoveNextAsync())
                {
                    foreach (var document in cursor.Current)
                    {
                        listToReturn.Add(document);
                    }
                }
            }

            return listToReturn;
        }

        private async Task<T> GetDocument(FilterDefinition<T> filterDefinition)
        {
            bool found = false;
            T objectToReturn = default(T);

            using (var cursor = await MongoCollection.FindAsync(filterDefinition))
            {
                while (await cursor.MoveNextAsync())
                {
                    foreach (var document in cursor.Current)
                    {
                        objectToReturn = document;
                        found = true;
                        break;
                    }

                    if (found)
                    {
                        break;
                    }
                }
            }

            return objectToReturn;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entry"></param>
        /// <param name="sensitive">Only used for Regex to determine case-sensitivity (false = in-sensitive, true = sensitive)</param>
        /// <returns></returns>
        private FilterDefinition<T> BuildFilterDefinition(BsonDocumentBuilder entry, bool sensitive = false)
        {
            switch (entry.Operator)
            {
                case Operator.GreaterThan:
                    return Builders<T>.Filter.Gt(entry.Key, entry.Value);
                case Operator.GreaterThanEquals:
                    return Builders<T>.Filter.Gte(entry.Key, entry.Value);
                case Operator.LessThan:
                    return Builders<T>.Filter.Lt(entry.Key, entry.Value);
                case Operator.LessThanEquals:
                    return Builders<T>.Filter.Lte(entry.Key, entry.Value);
                case Operator.Regex:
                    return Builders<T>.Filter.Regex(new StringFieldDefinition<T>(entry.Key), 
                        new BsonRegularExpression(new Regex(entry.Value.ToString(), !sensitive ? RegexOptions.IgnoreCase : RegexOptions.None)) );
                case Operator.NotEquals:
                    return Builders<T>.Filter.Ne(entry.Key, entry.Value);
                case Operator.Equals:
                default:
                    return Builders<T>.Filter.Eq(entry.Key, entry.Value);
            }
        }

        private BsonElement BuildBsonElement(string typeName, string key, object value)
        {
            switch (typeName)
            {
                case "Int16":
                    return new BsonElement(key, (short)value);
                case "Int32":
                    return new BsonElement(key, (int)value);
                case "Double":
                    return new BsonElement(key, (double)value);
                case "Boolean":
                    return new BsonElement(key, (bool)value);
                case "DateTime":
                    return new BsonElement(key, (DateTime)value);
                default:
                    return new BsonElement(key, (string)value);
            }
        }

        private void CheckOperatorIsAndOrOr(Operator operatorValue)
        {
            if (operatorValue != Operator.And && operatorValue != Operator.Or)
            {
                throw new ArgumentOutOfRangeException("Can only pass And or Or");
            }
        }

        private void CheckOperatorIsNotAndOrOrOrRegex(Operator operatorValue)
        {
            if (operatorValue != Operator.And && operatorValue != Operator.Or && operatorValue != Operator.Regex)
            {
                return;
            }

            if (operatorValue == Operator.Regex)
            {
                throw new ArgumentOutOfRangeException("For Regular Expressions. Use the Search method.");
            }

            throw new ArgumentOutOfRangeException("Cannot use And or Or. Must use Equals, Not Equals, Greater Than, Greater Than Equals, Less Than, Less Than Equals");
        }

        private void CheckOperatorIsNotAndOrOrOrRegex(IEnumerable<Operator> operators)
        {
            foreach (var operatorValue in operators)
            {
                CheckOperatorIsNotAndOrOrOrRegex(operatorValue);
            }
        }

        private void CheckOperatorsAreValid(IList<BsonDocumentBuilder> entries, Operator operatorValue)
        {
            CheckOperatorIsAndOrOr(operatorValue);
            CheckOperatorIsNotAndOrOrOrRegex(entries.Select(x => x.Operator));
        }

        private FilterDefinition<T> BuildFilterDefinitionWithOperator(IList<BsonDocumentBuilder> entries, Operator operatorValue)
        {
            IList<FilterDefinition<T>> filterDefinitions = entries.Select(entry => BuildFilterDefinition(entry)).ToList();
            return operatorValue == Operator.And ? Builders<T>.Filter.And(filterDefinitions) : Builders<T>.Filter.Or(filterDefinitions);
        }

        private ObjectId ValidateObjectId(string objectId)
        {
            ObjectId parsedObjectId;
            if (!ObjectId.TryParse(objectId, out parsedObjectId))
            {
                throw new ArgumentException("Object Id does not parse");
            }

            return parsedObjectId;
        }
    }
}