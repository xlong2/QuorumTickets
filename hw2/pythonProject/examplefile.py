print("this is an example")


from time import sleep
import pygame

name = ""
location = ""
school = "Willow High"
teacher = "Smith"
angsty = "Roxxi"
popular = "Tatiana"
nerdy = "Sophie"
angstyEyeColor = ""
angstyHairColor = ""
popularEyeColor = ""
popularHairColor = ""
nerdyEyeColor = ""
nerdyHairColor = ""


location = "class"

name = input("Hey. What's your name?")
sleep(1)

print("Location: " + location)
print("You’ve just moved to a new town and it’s your first day at " + school + ". You’ve just walked into your English class, your first class of the day.")
sleep(2)
print("You notice that all of the seats are filled up besides the one in the back. You make your way towards the seat.")
sleep(2)
print("The teacher walks in and once class begins, she starts talking about the objectives of the day.")
sleep(2)
print("“So today we are reading- oh!” her eyes land on you, “You must be our new student! " + name + ", right?”")
sleep(1)

print("Choice A: “Uh, yeah, that’s me”")
print("Choice B: “Yes, that is right, Mrs. " + teacher + "”")
print("Choice C: Just nod and say yes")
choice = input("What do you do?")

if choice.lower() == 'a':
    print("“Uh, yeah, that’s me”")
elif choice.lowe() == 'b':
    print("“Yes, that is right, Mrs. " + teacher + "”")
    print("You hear a scoff from the girl next to you.")
elif choice.lower() == 'c':
    print("Just nod and say yes")
sleep(1)

print("“Wonderful! Meet me after class and I can work with you to catch you up. We will be doing a partner activity today, but don’t worry, you should be fine.”")
sleep(2)
print("You nod in understanding and she continues to explain the assignment. Apparently you will be reading and analyzing a poem.")
sleep(2)
print("“Now get into pairs!”")
print("At this, you panic, not knowing anyone.")
print("You notice the girl next to you also seems to not have anyone to work with.")
sleep(1)

print("Choice A: Ask if she’d like to work together.")
print("Choice B: Wait to see if anyone else doesn’t have a partner.")#-1
choice = input("What do you do?")

if choice.lower() == 'a':
    print("You ask her if she would like to work together.")
    print("She looks at you straight-faced, “Whatever.”")
elif choice.lower() == 'b':
    print("You sit there and wait to see if anyone else does not have a partner.")
    print("After a few moments of waiting, everyone else seems to be partnered up. You sigh.")
    print("“Who doesn’t have a partner?” the teacher asks.")
    print("You slowly raise your hand. You notice the girl next to you also raises her hand, although halfheartedly.")
    print("“Oh, perfect! " + name + ", " + angsty + ", you two can work together!”")
    print("You sigh, turning to face the girl.")
    print("“I’m not that thrilled to be your partner either” she says with a roll of her eyes.")
sleep(2)
print("“Uh, ok… so I guess we should read the poem first and then we can discuss” you suggest.")
print("“Sure,” she says and you both turn read the poem on your own.")
sleep(2)
print("“Two roads diverged in a yellow wood,\n And sorry I could not travel both\n And be one traveler, long I stood\n And looked down one as far as I could…”")
sleep(3)
print("“...To where it bent in the undergrowth;\n Then took the other, as just as fair,\n And having perhaps the better claim,\n Because it was grassy and wanted wear…”")
sleep(3)
print("“...Though as for that the passing there\n Had worn them really about the same,\n And both that morning equally lay\n In leaves no step had trodden black…”")
sleep(3)
print("“...Oh, I kept the first for another day!\n Yet knowing how way leads on to way,\n I doubted if I should ever come back.\n I shall be telling this with a sigh…”")
sleep(3)
print("“...Somewhere ages and ages hence:\n Two roads diverged in a wood, and I-\n I took the one less traveled by,\n And that has made all the difference.”")
sleep(3)
print("You turn to the girl after finishing reading and notice she is on her phone")
sllep(2)

print("Choice A: “Were you even reading?”") #(-1)
print("Choice B: “So I’m assuming this was written by a hipster”")#(+1)
print("Choice C: “I guess this is about not wanting to follow the crowd”")
choice = input("What do you do?")

if choice.lower() == 'a':
    print("“Were you even reading?” you ask with a sigh.")
    print("“Yes, idiot, not everyone’s as slow of a reader as you”")
    print("Not knowing how to reply, you decide to ignore that and go into discussing the poem.")
elif choice.lower() == 'b':
    print("“So I’m assuming this was written by a hipster,” you joke.")
    print("She lightly chuckles, “Yeah, we get it. You’re different,” she says in reference to the author.")
    print("You laugh at that.")
elif choice.lower() == 'c':
    print("“I guess this is about not wanting to follow the crowd” you say with a shrug.")
    print("“Uh, yeah. That’s what I thought too.”")

sleep(2)
print("You realize she never gave you her name.")
print("“Hey, what was your name again?”, you ask.")
print("“I never gave it to you, haha. It’s " + angsty + ",” she responds sarcastically.")
sleep(2)
print("You both continue to discuss the poem until the teacher asks everyone to wrap up what they were saying so they can discuss it as a class.")
print("Towards the end of the discussion, the bell rings and everyone starts to gather their belongings.")
sleep(2)
print("Before getting up to make your way out of the classroom, you notice a girl dressed in a cheerleading uniform standing beside your desk")
print("You look up and realize she’s looking at you with a bright smile on her face.")
print("“Hi, I’m " + popular + "”")
sleep(2)
print("“Uh, hi, I’m " + name + "”")
print("“How’s you first day here so far?”")
print("“So far, it’s been alright”")
print("“That’s good. So, uh, listen…” she touches your shoulder. “You should sit with me and my friends at lunch.”")
sleep(2)

print("Choice A: “Sure, that sounds good. I’ll see you there.”")#+1
print("Choice B: “Uh, yeah, maybe.”")
print("Choice C: “I actually was planning on sitting with someone else.”")#-1
lunch = input("What do you do?")

if lunch.lower() == 'a':
    print("“Sure, that sounds good,” you say. “I’ll see you there.”")
    print("“Cool. See ya.”")
elif lunch.lower() == 'b':
    print("“Uh, yeah, maybe.”")
    print("“K. Hopefully I’ll see you then.”")
elif lunch.lower() == 'c':
    print("“I actually was planning on sitting with someone else.”")
    print("“Oh. What a shame. Maybe another day.”")

print("She walks away with a graceful sway to her hips.")
sleep(2)
print("After wrapping your head around what just happened, you pack up your stuff and head out of the classroom.")
location = "hallway"
print("On your way to your next class, you make a sharp turn around a corner and accidentally bump into someone. Books and papers fly everywhere.")
sleep(2)

print("Choice A: “Oh my god, I’m so sorry!”")
print("Choice B: “Hey, watch where you’re going!”")#-1
print("Choice C: Apologize and start picking up her books.")#+1
soph = input("What do you do?")

if soph.lower() == 'a':
    print("“Oh my god, I’m so sorry!”")
    print("You look up and see a cute girl dressed in pastels")
    print("“Ah, no, I’m fine it was partly my fault anyway,” she says")
elif soph.lower() == 'b':
    print("“Hey, watch where you’re going!”")
    print("You look up and see a cute girl dressed in pastels")
    print("“Oh, uh, sorry,” she says")
elif soph.lower() == 'c':
    print("Apologize and start picking up her books.")
    print("“Ah, don’t worry about it. I’m pretty clumsy myself. Thanks for helping me pick up my stuff,” she says")
    print("“Yeah, no problem.”")
    print("Only when you both are finished picking up all the books do you finally look at her.")
sleep(2)

print("You make eye contact and gaze into her " + nerdyEyeColor + " tinted eyes through her glasses lenses.")
print("She blushes and starts to get flustered. ‘W-what?!!’, she stutters.")
print("Choice A: “Oh nothing, sorry! I just started zoning out”")#(+0)
print("Choice B: “Uhh.. I was just noticing the color of your eyes.”")#(+1)
print("Choice C: “Nothing, just thought your hair looks kinda funny”")#(-1)
choice = input("How do you respond:")
if choice.lower() == 'a':
    print("“Oh nothing, sorry! I just started zoning out,” you say quickly.")
    print("“Oh… I’m sorry.” she says.")
elif choice.lower() == 'b':
    print("“Uhh.. I was just noticing the color of your eyes.” you reply.")
    print("“T-thanks...” she says, blushing. “Sorry. I have to get to class now.”")
    if soph,lower() != 'b':
        print("“It was nice meeting you.”")
elif choice.lower():
    print("“Nothing. I just thought your hair looks kinda funny.”")
    print("“Rude!!”")

sleep(2)
print("Before she walks away, you call to her, “Hey! I never got your name!”.")
print("She responds, “Oh sorry, it’s " + nerdy +".”")
print("You introduce yourself and she walks away quickly and nervously, her long " + nerdyHairColor + " hair swaying side to side.")
sleep(3)


#2. Eating Lunch - First hangout:
print("The bell rings for lunch. On your way to the cafeteria, you see a strange girl that you can’t recognize waving at you.")
print("“Hey, you’re the kid from my English class.” You turn and see" + angsty + "from your first class.")
print("“Uh, yeah.” you mutter. She asks you if you want to get lunch with her.")
print("Choice A: “No thanks, I’m eating with somebody else.”") #(-1)
print("Choice B: “Sure, I don’t have any friends here anyways.”") #(+1)
print("Choice C: “Maybe tomorrow? Sorry!”") #(+0)

lunch2 = input("How do you respond?")
if lunch2.lower() == 'a':
    print("“Actually, I’m eating with somebody else.”")
    print("“Okay, I didn’t know you were Mr. Popular... Whatever.”")
    print("She sounds kinda pissed.") #Maybe I shouldn’t have said that… I don’t even have anyone to sit with!
elif lunch2.lower() == 'b':
    print("“Sure thing. I didn’t have anyone to sit with anyways.”")
elif lunch2.lower() == 'c':
    print("“Oh, no problem. See you whenever.”")




location = "The Cafetaria"
if (lunch.lower() == 'a' or lunch.lower() == 'b') and (lunch2.lower() == 'a' or lunch2.lower() == 'c'):
    #Sitting with Tatiana...
    print("All the food looks like brown mush, and you can’t exactly make out what it is.")
    print("You pick the closest thing to pizza you can find.")
    print("You scan the room for " + popular + " and see her sitting at a big table with a lot of people.")
    print("She looks up and sees you.")
    print("“Hey, " + name + "! Come here!” She waves you over so you make your way to her table.")
    print("“Everybody, this is " + name + ". He just started here.”")
    print("All of her friends inbtroduce themselves to you.")

elif lunch.lower() == 'a' and lunch2.lower() == 'b':
    #Choosing between Tati and Roxxi
    print("All the food looks like brown mush, and you can’t exactly make out what it is.")
    print("You pick the closest thing to pizza you can find.")

elif (lunch.lower() == 'b' or lunch.lower() == 'c') and lunch2.lower() == 'b':
    #Sit with Roxxi but makes eyecontact with Tati
    print("All the food looks like brown mush, and you can’t exactly make out what it is.")
    print("You pick the closest thing to pizza you can find.")

elif lunch.lower() == 'c' and (lunch2.lower() == 'a' or lunch2.lower() == 'c'):
    #Scan the cafeteria and choose to sit alone.
    #Sophie will come and sit next to you.
    print("All the food looks like brown mush, and you can’t exactly make out what it is.")
    print("You pick the closest thing to pizza you can find.")
    print("You find a table close to the windows near the back that no one is sitting on")
