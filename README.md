Frame is an automatic immage collage program with no overlapping images.

It creates a single image with 1 or more images based on weighted tags of the images.

I initially created this for a family funeral, where I wanted images where the deceased was always pressent in the collage produced.
I wanted to exclude any ex-family members unless the rest of their family was also present in the image with them.

This would allow ex-family to show up in group photos, but not individually.
The rules got a little complex as to who could be shown next to who to avoid issues during the funeral while the images were being displayed.

To accomplish this I added tags to each image. Each person was given a tag with their name, then rules would create additional tags based
on the existance or absense of other tags.

One example is -

Some simple examples -

 - All images require the deceased to in them (tag with thier name is on the image).
 - No image can have the "skip" tag.
 - All images require the "pass" tag, which is given when -

 - Add tag "pass" when -
  - No ex-family is in the image.
 - Add tag "pass" when -
  - Ex-family is in the image, as well as two members of that persons family at the time.
  - IE, ex-uncle is OK only if their children are also in the image, but not just ex-uncle and deceased alone.

 - Skip tag is added when -
   - Specific, hostile people appear in a photo with other specific hotile people.
   - IE, two sistes who hate each other never want photos of themselves together.
   - Alone or with others there is no problem, just together and we end up with a fight.

Family at funerals are, interesting. From the point of a programming challenge I enjoyed it. Figuring out the family dynamics, less so.

This project grew, and eventually I ended up using it daily for other things.
I use it to create the backgorund on all my computers, TVs, mobile devices, etc.

Each generated image has its own rules, output size (ex, 1024x768), as well as output times (eg, new image every 30 seconds, or 5 minutes).

I would create images that mix family, pets, people I like, etc.
I could ensure the first image is always a family member, 2nd is always a pet, 3rd could be a friend,
and 4th+ images could be any of the above.

The source of these images can be configured to be a directory, website, basically anything that can be represented as a path using "io.fs".

The program is designed to scale. It can do it all as a single program, or as individual programs spread across various servers.
Caching can be added easily to any component, as all components are defined as interfaces.
