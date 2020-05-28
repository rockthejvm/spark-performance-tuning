package common

// Scenario: managing the data of a factory of handmade guitars like Taylor, Martin or Tanglewood
// Factories produce lots of guitars, and you can customize everything: wood types, decorations, materials, bracing etc
// Every instance of Guitar is a reference description of a guitar with certain features and a sound score from 0 to 5, considered "objective"
case class Guitar(
                   configurationId: String,
                   make: String,
                   model: String,
                   /* you can add additional fields here, I left this space blank for brevity */
                   soundScore: Double // factory ("new") score
                 )

// A GuitarSale object describes a certain guitar (identified by its registration) with a sound score (again, "objective") and the sale price on the store
case class GuitarSale(
                       registration: String,
                       make: String,
                       model: String,
                       soundScore: Double, // this can be higher or lower than "new" score (properly aged wood or badly treated guitar)
                       salePrice: Double
                     )
