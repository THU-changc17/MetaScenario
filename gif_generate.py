import imageio


def create_gif(image_list, gif_name, duration=0.65):
    frames = []
    for image_name in image_list:
        frames.append(imageio.imread(image_name))
    imageio.mimsave(gif_name, frames, 'GIF', duration=duration)
    return


def main():
    image_list = ['../MergingZS_example/35000.jpg', '../MergingZS_example/36000.jpg', '../MergingZS_example/37000.jpg', '../MergingZS_example/38000.jpg',
                  '../MergingZS_example/39000.jpg', '../MergingZS_example/40000.jpg', '../MergingZS_example/41000.jpg','../MergingZS_example/42000.jpg']
    gif_name = '../MergingZS_example/cat.gif'
    duration = 1.0
    create_gif(image_list, gif_name, duration)


if __name__ == '__main__':
    main()