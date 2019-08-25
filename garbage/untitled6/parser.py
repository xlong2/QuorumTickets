import pygame

path = ""
pygame.init() # now use display and fonts

display_width = 800
display_height = 600

largeText = pygame.font.Font("orangekid.ttf", 25)
black = (0,0,0)
white = (255,205,255)


gameDisplay = pygame.display.set_mode((display_width,display_height))
pygame.display.set_caption('A bit Racey')
clock = pygame.time.Clock()


def text_objects(text, font):
    textSurface = font.render(text, True, white)
    return textSurface, textSurface.get_rect()

running = 1

# fill in the dialogues
scripts = dict()
scripts[1]= "Hello"
scripts[2]= "Hello, nice to see you"

# set up the screen
screen = pygame.display.set_mode((display_width, display_height))

# iterate all the dialogs, only change to the next sentence upon user click
for ite in scripts.values():
    running = 1
    while running:
        pygame.display.flip()

        # ?maybe refill the text surface to erase the previous text.

        # read in any happened event
        event = pygame.event.poll()

        # make the corresponding text surface for display
        largeText = pygame.font.Font('orangekid.ttf',50)
        TextSurf, TextRect = text_objects(ite, largeText)

        # set display position
        TextRect.center = ((display_width/2),(display_height/2))

        # display
        gameDisplay.blit(TextSurf, TextRect)

        # update view
        pygame.display.update()
        pygame.display.flip() #keep the window active

        if event.type == pygame.MOUSEBUTTONDOWN:
            running = 0
